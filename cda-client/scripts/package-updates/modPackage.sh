#!/usr/bin/env bash

# Set version suffix from release input or environment.
version_suffix="${1:-${CDA_CLIENT_VERSION_SUFFIX:-${CWMSJS_VERSION_SUFFIX:-}}}"
if [ -z "$version_suffix" ]; then
  echo "Missing version suffix. Set CDA_CLIENT_VERSION_SUFFIX or pass a version suffix argument." >&2
  exit 1
fi

# Add any manual package.json updates from updates.json
npx node-jq '. * input' cwmsjs/package.json scripts/package-updates/updates.json |

# Copy selected fields from root package.json
npx node-jq --slurpfile root package.json \
'. * {author: $root[0].author, generatorVersion: $root[0].version, keywords: $root[0].keywords, repository: $root[0].repository}' |

# Write version as genVer-versionSuffix
# This will be used until CDA itself exposes an official CalVer version
npx node-jq --arg versionSuffix "$version_suffix" --slurpfile root package.json '. * {version: ($root[0].version + "-" + $versionSuffix)}' |

# Remove erroneous publishConfig entry
npx node-jq 'del(.publishConfig)' |

# Write to file
cat > temp.json
mv temp.json cwmsjs/package.json

# Copy custom README.md to generated package
cp README.md cwmsjs/README.md
