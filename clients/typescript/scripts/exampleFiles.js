const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const root = path.resolve(__dirname, '..');
function exampleFiles() {
  return ['endpoints', 'generator'].flatMap(directory =>
    fs.readdirSync(path.join(root, 'tests', directory))
      .filter(name => name.endsWith('.test.js'))
      .map(name => path.join('tests', directory, name))
  ).filter(file => !fs.readFileSync(path.join(root, file), 'utf8').includes('//!ignore')).sort();
}
function digest(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(path.join(root, file))).digest('hex');
}
module.exports = { root, exampleFiles, digest };
