const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { root, exampleFiles, digest } = require('./exampleFiles');
const files = exampleFiles();
const manifest = path.join(root, 'build', 'tested-examples.json');
fs.mkdirSync(path.dirname(manifest), { recursive: true });
fs.rmSync(manifest, { force: true });
if (!files.length) throw new Error('No documentation examples were found');
const result = spawnSync(process.execPath, [
  '--experimental-vm-modules', path.join(root, 'tests/node_modules/jest/bin/jest.js'),
  '--runInBand', '--ci', '--runTestsByPath', ...files.map(file => path.join(root, file)),
], { cwd: path.join(root, 'tests'), stdio: 'inherit' });
if (result.error) throw result.error;
if (result.status !== 0) process.exit(result.status || 1);
fs.writeFileSync(manifest, JSON.stringify(Object.fromEntries(files.map(file => [file.replaceAll('\\', '/'), digest(file)])), null, 2) + '\n');
