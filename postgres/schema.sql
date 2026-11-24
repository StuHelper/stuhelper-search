CREATE SCHEMA IF NOT EXISTS search;
CREATE TABLE IF NOT EXISTS search.documents (
 source text NOT NULL, id text NOT NULL, revision bigint NOT NULL CHECK(revision>0),
 data jsonb NOT NULL, PRIMARY KEY(source,id),
 CONSTRAINT documents_content_kind CHECK ((
   (source='catalog' AND data->'target'->>'kind' IN ('school','course','teacher'))
   OR (source='reviews' AND data->'target'->>'kind'='review')
   OR (source='resources' AND data->'target'->>'kind'='resource')
 ) IS TRUE)
);
-- Expand accepted catalog kinds only when needed; use NOT VALID + VALIDATE to avoid AccessExclusive full-table scans.
DO $$
DECLARE
 def text;
 valid boolean;
BEGIN
 SELECT pg_get_constraintdef(oid), convalidated
   INTO def, valid
   FROM pg_constraint
  WHERE conrelid = 'search.documents'::regclass
    AND conname = 'documents_content_kind';

 IF def IS NULL OR position('''school''' in def) = 0 THEN
  IF def IS NOT NULL THEN
   ALTER TABLE search.documents DROP CONSTRAINT documents_content_kind;
  END IF;
  ALTER TABLE search.documents ADD CONSTRAINT documents_content_kind CHECK ((
   (source='catalog' AND data->'target'->>'kind' IN ('school','course','teacher'))
   OR (source='reviews' AND data->'target'->>'kind'='review')
   OR (source='resources' AND data->'target'->>'kind'='resource')
  ) IS TRUE) NOT VALID;
  ALTER TABLE search.documents VALIDATE CONSTRAINT documents_content_kind;
 ELSIF NOT valid THEN
  ALTER TABLE search.documents VALIDATE CONSTRAINT documents_content_kind;
 END IF;
END $$;
CREATE INDEX IF NOT EXISTS search_visible_key ON search.documents(source,id) WHERE (data->>'visible')::boolean;
CREATE INDEX IF NOT EXISTS search_documents_visible_c_order ON search.documents(source COLLATE "C", id COLLATE "C") WHERE (data->>'visible')::boolean;
CREATE INDEX IF NOT EXISTS search_documents_kind_c_order ON search.documents((data->'target'->>'kind'), source COLLATE "C", id COLLATE "C") WHERE (data->>'visible')::boolean;
DO $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS pg_trgm;
  EXECUTE 'CREATE INDEX IF NOT EXISTS search_documents_title_trgm ON search.documents USING gin (lower(normalize(COALESCE(data->>''title'',''''), NFKC) COLLATE "pg_c_utf8") gin_trgm_ops) WHERE (data->>''visible'')::boolean';
EXCEPTION WHEN insufficient_privilege OR undefined_file OR feature_not_supported THEN
  NULL;
END
$$;
DO $$
BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='search.documents'::regclass AND attname='at_utc' AND NOT attisdropped) THEN
  ALTER TABLE search.documents ADD COLUMN at_utc timestamptz;
  UPDATE search.documents SET at_utc=(data->>'at')::timestamptz WHERE jsonb_typeof(data->'at')='string';
 END IF;
END $$;
CREATE INDEX IF NOT EXISTS search_visible_recent ON search.documents(at_utc DESC, source COLLATE "C" ASC, id COLLATE "C" ASC) WHERE (data->>'visible')::boolean AND at_utc IS NOT NULL;
CREATE INDEX IF NOT EXISTS search_visible_at_text ON search.documents((data->>'at') DESC, source ASC, id ASC) WHERE (data->>'visible')::boolean;
CREATE TABLE IF NOT EXISTS search.inbox (
 source text NOT NULL, id text NOT NULL, event_type text NOT NULL DEFAULT '',
 digest text NOT NULL, received_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(source,event_type,id)
);
DO $$
BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='search.inbox'::regclass AND attname='event_type' AND NOT attisdropped) THEN
  ALTER TABLE search.inbox ADD COLUMN event_type text NOT NULL DEFAULT '';
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='search.inbox'::regclass AND attname='received_at' AND NOT attisdropped) THEN
  ALTER TABLE search.inbox ADD COLUMN received_at timestamptz NOT NULL DEFAULT now();
 END IF;
 IF EXISTS (
  SELECT 1 FROM pg_constraint
  WHERE conrelid = 'search.inbox'::regclass AND contype = 'p' AND array_length(conkey, 1) = 2
 ) THEN
  ALTER TABLE search.inbox DROP CONSTRAINT inbox_pkey;
  ALTER TABLE search.inbox ADD PRIMARY KEY (source, event_type, id);
 END IF;
END $$;
CREATE INDEX IF NOT EXISTS search_inbox_received_at ON search.inbox(received_at, source, event_type, id);
CREATE TABLE IF NOT EXISTS search.state (id boolean PRIMARY KEY DEFAULT true CHECK(id), ready boolean NOT NULL, cursor_key bytea);
DO $$
BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='search.state'::regclass AND attname='cursor_key' AND NOT attisdropped) THEN
  ALTER TABLE search.state ADD COLUMN cursor_key bytea;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='search.state'::regclass AND attname='rebuilt_at' AND NOT attisdropped) THEN
  ALTER TABLE search.state ADD COLUMN rebuilt_at timestamptz;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='search.state'::regclass AND attname='activity_from' AND NOT attisdropped) THEN
  ALTER TABLE search.state ADD COLUMN activity_from date;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='search.state'::regclass AND attname='activity_to' AND NOT attisdropped) THEN
  ALTER TABLE search.state ADD COLUMN activity_to date;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='search.state'::regclass AND attname='activity_calendar' AND NOT attisdropped) THEN
  ALTER TABLE search.state ADD COLUMN activity_calendar text NOT NULL DEFAULT 'UTC';
 END IF;
END $$;
INSERT INTO search.state(id,ready) VALUES(true,false) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS search.activities (
 source text NOT NULL CHECK(source IN ('catalog','reviews','favorites')),
 id text NOT NULL, kind text NOT NULL,
 object_id text NOT NULL, day date NOT NULL, revision bigint NOT NULL CHECK(revision>0),
 data jsonb NOT NULL, PRIMARY KEY(source,id), UNIQUE(source,kind,object_id,day),
 CONSTRAINT activities_target_kind CHECK (kind IN ('course','teacher','resource'))
);
-- Expand accepted activity target kinds on upgrade while preserving existing rows.
DO $$
BEGIN
 IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='search.activities'::regclass AND conname='activities_kind_check') THEN
  ALTER TABLE search.activities DROP CONSTRAINT activities_kind_check;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='search.activities'::regclass AND conname='activities_target_kind') THEN
  ALTER TABLE search.activities ADD CONSTRAINT activities_target_kind CHECK (kind IN ('course','teacher','resource')) NOT VALID;
  ALTER TABLE search.activities VALIDATE CONSTRAINT activities_target_kind;
 END IF;
END $$;
CREATE INDEX IF NOT EXISTS search_activity_window ON search.activities(kind,day);
CREATE INDEX IF NOT EXISTS search_activities_day_key ON search.activities(day, source, id);
CREATE INDEX IF NOT EXISTS search_activities_by_day_c ON search.activities(kind, day, source COLLATE "C", id COLLATE "C") WHERE (data->>'visible')::boolean;
CREATE TABLE IF NOT EXISTS search.activity_calendar_archive (
 calendar text NOT NULL CHECK(calendar IN ('UTC','Asia/Shanghai')),
 source text NOT NULL, id text NOT NULL, kind text NOT NULL, object_id text NOT NULL,
 day date NOT NULL, revision bigint NOT NULL, data jsonb NOT NULL,
 PRIMARY KEY(calendar,source,id)
);
DO $$
DECLARE
 tbl text;
BEGIN
 IF has_schema_privilege('public', 'search', 'USAGE,CREATE') THEN
  REVOKE ALL ON SCHEMA search FROM PUBLIC;
 END IF;
 -- The deployment role configurator grants CRUD on the whole schema. With no
 -- policies, RLS reserves the archive for its owner even under those grants.
 IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid='search.activity_calendar_archive'::regclass) THEN
  ALTER TABLE search.activity_calendar_archive ENABLE ROW LEVEL SECURITY;
 END IF;
 FOREACH tbl IN ARRAY ARRAY['search.documents', 'search.inbox', 'search.state', 'search.activities', 'search.activity_calendar_archive'] LOOP
  IF EXISTS (SELECT 1 FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) acl
             WHERE c.oid=tbl::regclass AND acl.grantee=0) THEN
   EXECUTE format('REVOKE ALL ON TABLE %s FROM PUBLIC', tbl);
  END IF;
 END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS search.rankings (
 slot smallint PRIMARY KEY CHECK(slot BETWEEN 1 AND 1024),
 token text NOT NULL UNIQUE,
 targets jsonb NOT NULL CHECK(jsonb_typeof(targets)='array' AND jsonb_array_length(targets)<=10000),
 expires_at timestamptz NOT NULL
);
REVOKE ALL ON search.rankings FROM PUBLIC;

CREATE TABLE IF NOT EXISTS search.retired_sources (source text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS search.document_holds (
 source text NOT NULL,id text NOT NULL,case_ref text NOT NULL,basis text NOT NULL,purpose text NOT NULL,end_condition text NOT NULL,
 review_at timestamptz NOT NULL,until_at timestamptz NOT NULL CHECK(isfinite(until_at) AND review_at<=until_at),
 PRIMARY KEY(source,id,case_ref),FOREIGN KEY(source,id) REFERENCES search.documents(source,id) ON DELETE CASCADE
);
REVOKE ALL ON search.document_holds FROM PUBLIC;

CREATE OR REPLACE FUNCTION search.retire_confirmed_revision(p_source text,p_id text,p_revision bigint)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE stored bigint; visible boolean;
BEGIN
 IF p_source NOT IN ('catalog','reviews','resources') OR p_id IS NULL OR p_id='' OR p_revision IS NULL OR p_revision<1 THEN
   RAISE EXCEPTION 'invalid retirement' USING ERRCODE='22023';
 END IF;
 PERFORM pg_advisory_xact_lock(726391840130);
 SELECT d.revision,(d.data->>'visible')::boolean INTO stored,visible FROM search.documents d
  WHERE d.source=p_source AND d.id=p_id FOR UPDATE;
 IF FOUND AND (stored<p_revision OR (stored=p_revision AND visible)) THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM search.document_holds h WHERE h.source=p_source AND h.id=p_id AND h.until_at>statement_timestamp()) THEN RETURN false; END IF;
 INSERT INTO search.retired_sources(source) VALUES(p_source) ON CONFLICT DO NOTHING;
 IF stored=p_revision THEN DELETE FROM search.documents WHERE source=p_source AND id=p_id AND revision=p_revision; END IF;
 RETURN true;
END $$;
REVOKE ALL ON FUNCTION search.retire_confirmed_revision(text,text,bigint) FROM PUBLIC;
