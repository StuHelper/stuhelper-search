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
-- Expand the accepted catalog kinds without deleting existing projections.
ALTER TABLE search.documents DROP CONSTRAINT IF EXISTS documents_content_kind;
ALTER TABLE search.documents ADD CONSTRAINT documents_content_kind CHECK ((
 (source='catalog' AND data->'target'->>'kind' IN ('school','course','teacher'))
 OR (source='reviews' AND data->'target'->>'kind'='review')
 OR (source='resources' AND data->'target'->>'kind'='resource')
) IS TRUE);
CREATE INDEX IF NOT EXISTS search_visible_key ON search.documents(source,id) WHERE (data->>'visible')::boolean;
CREATE INDEX IF NOT EXISTS search_visible_kind_key ON search.documents((data->'target'->>'kind'), source COLLATE "C", id COLLATE "C") WHERE (data->>'visible')::boolean;
DO $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS pg_trgm;
  EXECUTE 'CREATE INDEX IF NOT EXISTS search_documents_title_trgm ON search.documents USING gin (lower(normalize(COALESCE(data->>''title'',''''), NFKC) COLLATE "pg_c_utf8") gin_trgm_ops) WHERE (data->>''visible'')::boolean';
EXCEPTION WHEN insufficient_privilege OR undefined_file OR feature_not_supported THEN
  NULL;
END
$$;
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
REVOKE ALL ON SCHEMA search FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA search FROM PUBLIC;
