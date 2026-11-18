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
CREATE TABLE IF NOT EXISTS search.inbox (
 source text NOT NULL, id text NOT NULL, digest text NOT NULL, PRIMARY KEY(source,id)
);
CREATE TABLE IF NOT EXISTS search.state (id boolean PRIMARY KEY DEFAULT true CHECK(id), ready boolean NOT NULL);
INSERT INTO search.state(id,ready) VALUES(true,false) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS search.activities (
 source text NOT NULL CHECK(source IN ('catalog','reviews','favorites')),
 id text NOT NULL, kind text NOT NULL CHECK(kind IN ('course','teacher')),
 object_id text NOT NULL, day date NOT NULL, revision bigint NOT NULL CHECK(revision>0),
 data jsonb NOT NULL, PRIMARY KEY(source,id), UNIQUE(source,kind,object_id,day)
);
CREATE INDEX IF NOT EXISTS search_activity_window ON search.activities(kind,day);
DO $$
DECLARE
 tbl text;
BEGIN
 IF has_schema_privilege('public', 'search', 'USAGE,CREATE') THEN
  REVOKE ALL ON SCHEMA search FROM PUBLIC;
 END IF;
 FOREACH tbl IN ARRAY ARRAY['search.documents', 'search.inbox', 'search.state', 'search.activities'] LOOP
  IF EXISTS (SELECT 1 FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) acl
             WHERE c.oid=tbl::regclass AND acl.grantee=0) THEN
   EXECUTE format('REVOKE ALL ON TABLE %s FROM PUBLIC', tbl);
  END IF;
 END LOOP;
END $$;
