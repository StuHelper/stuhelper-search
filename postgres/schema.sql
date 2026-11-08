CREATE SCHEMA IF NOT EXISTS search;
CREATE TABLE IF NOT EXISTS search.documents (
 source text NOT NULL, id text NOT NULL, revision bigint NOT NULL CHECK(revision>0),
 data jsonb NOT NULL, PRIMARY KEY(source,id)
);
CREATE INDEX IF NOT EXISTS search_visible_key ON search.documents(source,id) WHERE (data->>'visible')::boolean;
CREATE TABLE IF NOT EXISTS search.inbox (
 source text NOT NULL, id text NOT NULL, digest text NOT NULL, PRIMARY KEY(source,id)
);
CREATE TABLE IF NOT EXISTS search.state (id boolean PRIMARY KEY DEFAULT true CHECK(id), ready boolean NOT NULL);
INSERT INTO search.state(id,ready) VALUES(true,false) ON CONFLICT DO NOTHING;
REVOKE ALL ON SCHEMA search FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA search FROM PUBLIC;
