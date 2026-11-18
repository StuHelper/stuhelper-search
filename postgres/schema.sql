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
CREATE INDEX IF NOT EXISTS search_documents_visible_c_order ON search.documents(source COLLATE "C", id COLLATE "C") WHERE (data->>'visible')::boolean;
CREATE INDEX IF NOT EXISTS search_documents_kind_c_order ON search.documents((data->'target'->>'kind'), source COLLATE "C", id COLLATE "C") WHERE (data->>'visible')::boolean;
CREATE TABLE IF NOT EXISTS search.inbox (
 source text NOT NULL, id text NOT NULL, digest text NOT NULL, PRIMARY KEY(source,id)
);
CREATE TABLE IF NOT EXISTS search.state (id boolean PRIMARY KEY DEFAULT true CHECK(id), ready boolean NOT NULL, cursor_key bytea);
ALTER TABLE search.state ADD COLUMN IF NOT EXISTS cursor_key bytea;
INSERT INTO search.state(id,ready) VALUES(true,false) ON CONFLICT DO NOTHING;
UPDATE search.state SET cursor_key = sha256((random()::text || clock_timestamp()::text || pg_backend_pid()::text)::bytea) WHERE id AND cursor_key IS NULL;
CREATE TABLE IF NOT EXISTS search.activities (
 source text NOT NULL CHECK(source IN ('catalog','reviews','favorites')),
 id text NOT NULL, kind text NOT NULL CHECK(kind IN ('course','teacher')),
 object_id text NOT NULL, day date NOT NULL, revision bigint NOT NULL CHECK(revision>0),
 data jsonb NOT NULL, PRIMARY KEY(source,id), UNIQUE(source,kind,object_id,day)
);
CREATE INDEX IF NOT EXISTS search_activity_window ON search.activities(kind,day);
CREATE INDEX IF NOT EXISTS search_activities_window_c_order ON search.activities(kind, source COLLATE "C", id COLLATE "C", day) WHERE (data->>'visible')::boolean;
REVOKE ALL ON SCHEMA search FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA search FROM PUBLIC;
