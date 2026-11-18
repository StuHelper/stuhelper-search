package search_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	s "github.com/StuHelper/stuhelper-search"
	"github.com/StuHelper/stuhelper-search/postgres"
	"os"
	"testing"
	"time"
)

// DS-07: Repeated Migrate does not block online SELECT readers with AccessExclusiveLock, and CheckRuntime enforces UPDATE, TRUNCATE, and cross-schema isolation.
func TestIdempotentMigrateLockFreeAndStrictRuntimePrivilegeChecks(t *testing.T) {
	svc, repo, f, db := setup(t)
	ctx := context.Background()
	add(t, svc, f, doc("catalog", "course", "online-course"))

	// A-07: Hold an active online SELECT transaction (AccessShareLock on search.documents and search.activities) on one connection,
	// while running Migrate on a separate connection. Idempotent Migrate must complete without blocking on AccessExclusiveLock.
	dsn := os.Getenv("STUHELPER_SEARCH_TEST_DSN")
	var dbName string
	if err := db.QueryRow("SELECT current_database()").Scan(&dbName); err != nil {
		t.Fatal(err)
	}
	readerDB, err := sql.Open("pgx", dsn+" dbname="+dbName)
	if err != nil {
		t.Fatal(err)
	}
	defer readerDB.Close()

	readTx, err := readerDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer readTx.Rollback()

	var visibleCount int
	if err := readTx.QueryRowContext(ctx, "SELECT count(*) FROM search.documents").Scan(&visibleCount); err != nil || visibleCount != 1 {
		t.Fatalf("reader SELECT failed: count=%d err=%v", visibleCount, err)
	}
	if err := readTx.QueryRowContext(ctx, "SELECT count(*) FROM search.activities").Scan(&visibleCount); err != nil {
		t.Fatalf("reader activities SELECT failed: %v", err)
	}

	migCtx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer cancel()
	if err := postgres.Migrate(migCtx, db); err != nil {
		t.Fatalf("idempotent Migrate blocked on online SELECT transaction: %v", err)
	}
	_ = readTx.Rollback()

	// A-08: Verify CheckRuntime rejects missing UPDATE, allowed TRUNCATE, and cross-module schema/table access,
	// while accepting both minimal runtime role (no INSERT on state) and backend-style runtime role.
	if _, err := db.Exec(`CREATE SCHEMA catalog_private; CREATE TABLE catalog_private.courses(id text PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	checkWithRole := func(name, setupSQL string) error {
		t.Helper()
		role := fmt.Sprintf("rt_%s_%d", name, time.Now().UnixNano())
		if _, err := db.Exec("CREATE ROLE " + role + " NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = db.Exec("RESET ROLE")
			_, _ = db.Exec("REASSIGN OWNED BY " + role + " TO CURRENT_USER")
			_, _ = db.Exec("DROP OWNED BY " + role)
			_, _ = db.Exec("DROP ROLE IF EXISTS " + role)
		})
		if _, err := db.Exec(fmt.Sprintf(setupSQL, role)); err != nil {
			t.Fatal(name, err)
		}
		db.SetMaxOpenConns(1)
		if _, err := db.Exec("SET ROLE " + role); err != nil {
			t.Fatal(err)
		}
		defer db.Exec("RESET ROLE")
		return repo.CheckRuntime(ctx)
	}

	// 1. Minimal valid role (no INSERT on search.state, no DELETE anywhere) MUST pass.
	if err := checkWithRole("minimal_valid", `
		GRANT USAGE ON SCHEMA search TO %[1]s;
		GRANT SELECT,INSERT,UPDATE ON search.documents,search.activities TO %[1]s;
		GRANT SELECT,UPDATE ON search.state TO %[1]s;
		GRANT SELECT,INSERT ON search.inbox TO %[1]s;
	`); err != nil {
		t.Fatalf("minimal valid runtime role rejected: %v", err)
	}

	const minimum = `GRANT USAGE ON SCHEMA search TO %[1]s;
		GRANT SELECT,INSERT,UPDATE ON search.documents,search.activities TO %[1]s;
		GRANT SELECT,UPDATE ON search.state TO %[1]s;
		GRANT SELECT,INSERT ON search.inbox TO %[1]s;`
	if err := checkWithRole("replication", minimum+"ALTER ROLE %[1]s REPLICATION"); !errors.Is(err, s.ErrUnavailable) {
		t.Errorf("replication-capable identity accepted: %v", err)
	}
	if _, err := db.Exec("GRANT CREATE ON SCHEMA public TO PUBLIC"); err != nil {
		t.Fatal(err)
	}
	if err := checkWithRole("public_create", minimum); !errors.Is(err, s.ErrUnavailable) {
		t.Errorf("public CREATE accepted: %v", err)
	}
	if _, err := db.Exec("REVOKE CREATE ON SCHEMA public FROM PUBLIC"); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := db.QueryRow("SHOW server_version_num").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version >= 160000 {
		owner := fmt.Sprintf("rt_table_owner_%d", time.Now().UnixNano())
		if _, err := db.Exec("CREATE ROLE " + owner + " NOLOGIN; CREATE TABLE search.owner_check(id int); ALTER TABLE search.owner_check OWNER TO " + owner); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS search.owner_check; DROP ROLE IF EXISTS " + owner) })
		if err := checkWithRole("can_set_owner", minimum+"GRANT "+owner+" TO %[1]s WITH INHERIT FALSE, SET TRUE"); !errors.Is(err, s.ErrUnavailable) {
			t.Errorf("owner membership accepted: %v", err)
		}
	}
	if version >= 170000 {
		if _, err := db.Exec("GRANT MAINTAIN ON search.documents TO PUBLIC"); err != nil {
			t.Fatal(err)
		}
		if err := postgres.Migrate(ctx, db); err != nil {
			t.Fatal(err)
		}
		var allowed bool
		if err := db.QueryRow("SELECT has_table_privilege('public','search.documents','MAINTAIN')").Scan(&allowed); err != nil || allowed {
			t.Errorf("PUBLIC MAINTAIN survived migration: %v %v", allowed, err)
		}
	}

	// 2. Backend-style valid role (DELETE on projections, REVOKE UPDATE,DELETE on inbox, no TRUNCATE) MUST pass.
	if err := checkWithRole("backend_valid", `
		GRANT USAGE ON SCHEMA search TO %[1]s;
		GRANT SELECT,INSERT,UPDATE,DELETE ON search.documents,search.activities,search.state TO %[1]s;
		GRANT SELECT,INSERT ON search.inbox TO %[1]s;
	`); err != nil {
		t.Fatalf("backend-compatible runtime role rejected: %v", err)
	}

	// 3. Missing UPDATE on documents/activities/state MUST be rejected.
	for _, missingUpdateSQL := range []string{
		`GRANT USAGE ON SCHEMA search TO %[1]s; GRANT SELECT,INSERT ON search.documents,search.inbox TO %[1]s; GRANT SELECT,INSERT,UPDATE ON search.activities,search.state TO %[1]s;`,
		`GRANT USAGE ON SCHEMA search TO %[1]s; GRANT SELECT,INSERT,UPDATE ON search.documents,search.state TO %[1]s; GRANT SELECT,INSERT ON search.activities,search.inbox TO %[1]s;`,
		`GRANT USAGE ON SCHEMA search TO %[1]s; GRANT SELECT,INSERT,UPDATE ON search.documents,search.activities TO %[1]s; GRANT SELECT,INSERT ON search.state,search.inbox TO %[1]s;`,
	} {
		if err := checkWithRole("no_update", missingUpdateSQL); !errors.Is(err, s.ErrUnavailable) {
			t.Fatalf("role missing UPDATE was accepted: %v", err)
		}
	}

	// 4. TRUNCATE on inbox or documents MUST be rejected.
	for _, truncateSQL := range []string{
		`GRANT USAGE ON SCHEMA search TO %[1]s; GRANT SELECT,INSERT,UPDATE ON search.documents,search.activities,search.state TO %[1]s; GRANT SELECT,INSERT,TRUNCATE ON search.inbox TO %[1]s;`,
		`GRANT USAGE ON SCHEMA search TO %[1]s; GRANT SELECT,INSERT,UPDATE,TRUNCATE ON search.documents TO %[1]s; GRANT SELECT,INSERT,UPDATE ON search.activities,search.state TO %[1]s; GRANT SELECT,INSERT ON search.inbox TO %[1]s;`,
	} {
		if err := checkWithRole("with_truncate", truncateSQL); !errors.Is(err, s.ErrUnavailable) {
			t.Fatalf("role with TRUNCATE privilege was accepted: %v", err)
		}
	}

	// 5. Cross-module schema USAGE or table SELECT MUST be rejected.
	for _, crossModuleSQL := range []string{
		`GRANT USAGE ON SCHEMA search,catalog_private TO %[1]s; GRANT SELECT,INSERT,UPDATE ON search.documents,search.activities,search.state TO %[1]s; GRANT SELECT,INSERT ON search.inbox TO %[1]s;`,
		`GRANT USAGE ON SCHEMA search,catalog_private TO %[1]s; GRANT SELECT ON catalog_private.courses TO %[1]s; GRANT SELECT,INSERT,UPDATE ON search.documents,search.activities,search.state TO %[1]s; GRANT SELECT,INSERT ON search.inbox TO %[1]s;`,
	} {
		if err := checkWithRole("cross_schema", crossModuleSQL); !errors.Is(err, s.ErrUnavailable) {
			t.Fatalf("role with cross-module schema/table access was accepted: %v", err)
		}
	}
}
