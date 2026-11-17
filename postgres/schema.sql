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
ALTER TABLE search.documents ADD COLUMN IF NOT EXISTS at_utc timestamptz;
UPDATE search.documents SET at_utc = (data->>'at')::timestamptz WHERE at_utc IS NULL AND jsonb_typeof(data->'at') = 'string';
CREATE INDEX IF NOT EXISTS search_visible_recent ON search.documents(at_utc DESC, source COLLATE "C" ASC, id COLLATE "C" ASC) WHERE (data->>'visible')::boolean AND at_utc IS NOT NULL;
CREATE INDEX IF NOT EXISTS search_visible_at_text ON search.documents((data->>'at') DESC, source ASC, id ASC) WHERE (data->>'visible')::boolean;
CREATE TABLE IF NOT EXISTS search.inbox (
 source text NOT NULL, id text NOT NULL, digest text NOT NULL, PRIMARY KEY(source,id)
);
CREATE TABLE IF NOT EXISTS search.state (id boolean PRIMARY KEY DEFAULT true CHECK(id), ready boolean NOT NULL);
INSERT INTO search.state(id,ready) VALUES(true,false) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS search.activities (
 source text NOT NULL CHECK(source IN ('catalog','reviews','favorites')),
 id text NOT NULL, kind text NOT NULL,
 object_id text NOT NULL, day date NOT NULL, revision bigint NOT NULL CHECK(revision>0),
 data jsonb NOT NULL, PRIMARY KEY(source,id), UNIQUE(source,kind,object_id,day),
 CONSTRAINT activities_target_kind CHECK (kind IN ('course','teacher','resource'))
);
-- Expand accepted activity target kinds on upgrade while preserving existing rows.
ALTER TABLE search.activities DROP CONSTRAINT IF EXISTS activities_kind_check;
ALTER TABLE search.activities DROP CONSTRAINT IF EXISTS activities_target_kind;
ALTER TABLE search.activities ADD CONSTRAINT activities_target_kind CHECK (kind IN ('course','teacher','resource'));
CREATE INDEX IF NOT EXISTS search_activity_window ON search.activities(kind,day);
REVOKE ALL ON SCHEMA search FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA search FROM PUBLIC;
