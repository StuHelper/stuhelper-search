import fs from 'node:fs';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import YAML from 'yaml';
import assert from 'node:assert/strict';

function stripGoCommentsAndStrings(src) {
  return src
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/\/\/[^\n]*/g, ' ')
    .replace(/`[^`]*`/g, '""')
    .replace(/"(?:\\.|[^"\\])*"/g, '""');
}

const files = fs.readdirSync('.', { recursive: true }).filter((f) => f.endsWith('.go') && !f.startsWith('node_modules/'));
for (const f of files) {
  if (!f.startsWith('postgres/') && !f.endsWith('_test.go')) {
    const raw = fs.readFileSync(f, 'utf8');
    const codeOnly = stripGoCommentsAndStrings(raw);
    assert(!/\b(?:SELECT\s+.+\s+FROM|INSERT\s+INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM|CREATE\s+TABLE)\b/i.test(raw), `SQL outside repository: ${f}`);
    assert(!/\b(?:sql\.DB|sql\.Tx|pgx)\b/.test(codeOnly), `Database handle outside repository: ${f}`);
  }
}

const goListRaw = execFileSync('go', ['list', '-json', './...'], { encoding: 'utf8' });
const packages = goListRaw
  .trim()
  .split(/\n(?=\{)/)
  .map((chunk) => JSON.parse(chunk));
for (const pkg of packages) {
  if (pkg.ImportPath === 'github.com/StuHelper/stuhelper-search/postgres') {
    continue;
  }
  for (const imp of pkg.Imports ?? []) {
    assert(
      imp !== 'database/sql' && !imp.startsWith('github.com/jackc/pgx'),
      `Forbidden database import ${imp} in ${pkg.ImportPath}`,
    );
  }
}

const manifest = YAML.parse(fs.readFileSync('module.yaml', 'utf8'));
for (const [scenario, tests] of Object.entries(manifest.scenario_tests)) {
  assert(tests.length, `No test for ${scenario}`);
  for (const reference of tests) {
    const [file, name] = reference.split('#');
    assert(fs.readFileSync(file, 'utf8').includes(`func ${name}(`), `Missing scenario test: ${reference}`);
  }
}
console.log('PASS: repository SQL/import boundary and registered scenario functions');

const eventPath = 'contracts/discovery-event.schema.json';
const eventBytes = fs.readFileSync(eventPath);
assert.equal(
  fs.readFileSync(eventPath + '.sha256', 'utf8'),
  createHash('sha256').update(eventBytes).digest('hex') + '  discovery-event.schema.json\n',
);
const eventSchema = JSON.parse(eventBytes);
assert.equal(eventSchema.properties.specversion.const, '1.0');
assert.deepEqual(eventSchema.properties.source.enum, ['catalog', 'reviews', 'resources']);
assert.deepEqual(eventSchema.properties.type.enum, ['catalog.discovery.v1', 'reviews.discovery.v1', 'resources.discovery.v1']);
assert.deepEqual(eventSchema.properties.data.properties.target.properties.kind.enum, ['school', 'course', 'teacher', 'review', 'resource']);
console.log('PASS: discovery event schema structure and SHA-256');

const activityPath = 'contracts/discovery-activity.schema.json';
const activityBytes = fs.readFileSync(activityPath);
assert.equal(
  fs.readFileSync(activityPath + '.sha256', 'utf8'),
  createHash('sha256').update(activityBytes).digest('hex') + '  discovery-activity.schema.json\n',
);
const activitySchema = JSON.parse(activityBytes);
assert.equal(activitySchema['x-schema-version'], '1.1.0');
assert.deepEqual(
  activitySchema.oneOf.map((v) => [v.properties.source.const, Object.keys(v.properties.data.properties.metrics.properties)]),
  [
    ['catalog', ['views']],
    ['reviews', ['reviews', 'interactions']],
    ['favorites', ['favorites']],
  ],
);
console.log('PASS: activity schema version, ownership and SHA-256');
