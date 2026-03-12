#!/usr/bin/env bash

# Set date variable for use in version
printf -v date '%(%Y.%m.%d)T'

# Add any manual package.json updates from updates.json
npx node-jq '. * input' cwmsjs/package.json scripts/package-updates/updates.json |

# Copy selected fields from root package.json
npx node-jq --slurpfile root package.json \
'. * {author: $root[0].author, generatorVersion: $root[0].version, keywords: $root[0].keywords, repository: $root[0].repository}' |

# Write version as genVer-todaysDate
# This will be used until CDA itself exposes an official CalVer version
npx node-jq --arg date "$date" --slurpfile root package.json '. * {version: ($root[0].version + "-" + $date)}' |

# Remove erroneous publishConfig entry
npx node-jq 'del(.publishConfig)' |

# Write to file
cat > temp.json
mv temp.json cwmsjs/package.json

# Copy custom README.md to generated package
cp README.md cwmsjs/README.md