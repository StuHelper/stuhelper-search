import fs from 'node:fs';
import {createHash} from 'node:crypto';
import YAML from 'yaml';
import assert from 'node:assert/strict';
const files = fs.readdirSync('.', {recursive:true}).filter(f => f.endsWith('.go') && !f.startsWith('node_modules/'));
for(const f of files) { const s=fs.readFileSync(f,'utf8'); if (!f.startsWith('postgres/') && !f.endsWith('_test.go')) assert(!/\b(?:SELECT|INSERT INTO|UPDATE [a-z]|DELETE FROM|CREATE TABLE)\b/.test(s), `SQL outside repository: ${f}`); }

const manifest=YAML.parse(fs.readFileSync('module.yaml','utf8'));
for(const [scenario,tests] of Object.entries(manifest.scenario_tests)){
 assert(tests.length,`No test for ${scenario}`);
 for(const reference of tests){const [file,name]=reference.split('#');assert(fs.readFileSync(file,'utf8').includes(`func ${name}(`),`Missing scenario test: ${reference}`);}
}
console.log('PASS: repository SQL boundary and registered scenario functions');

const activityPath='contracts/discovery-activity.schema.json';
const activityBytes=fs.readFileSync(activityPath);
assert.equal(fs.readFileSync(activityPath+'.sha256','utf8'),createHash('sha256').update(activityBytes).digest('hex')+'  discovery-activity.schema.json\n');
const activitySchema=JSON.parse(activityBytes);
assert.equal(activitySchema['x-schema-version'],'1.0.0');
assert.deepEqual(activitySchema.oneOf.map(v=>[v.properties.source.const,Object.keys(v.properties.data.properties.metrics.properties)]),[['catalog',['views']],['reviews',['reviews','interactions']],['favorites',['favorites']]]);
console.log('PASS: activity schema version, ownership and SHA-256');
