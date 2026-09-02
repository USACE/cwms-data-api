#!/usr/bin/env bash
set -euo pipefail

git config --global --add safe.directory "$PWD"

# Docker Desktop bind mounts preserve Windows line endings and expose every file as executable.
# Match Git for Windows so the same checkout stays clean inside the container.
if grep -qi microsoft /proc/version; then
    git config --global core.autocrlf true
fi

# npm 10 strips platform metadata from the committed lockfile during the Gradle GUI build.
npm install --global npm@11

./gradlew --version
