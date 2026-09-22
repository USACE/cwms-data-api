#!/usr/bin/env bash

# Set date variable for use in version
printf -v date '%(%Y.%m.%d)T'

# Add default servers to spec
npx node-jq '.servers = inputs.servers' cwms-swagger-raw.json scripts/spec-updates/servers.json |

# Write version as genVer-todaysDate
# This will be used until CDA itself exposes an official CalVer version
npx node-jq --arg date "$date" --slurpfile root package.json '.info.version = ($root[0].version + "-" + $date)' |

# Add BaseRatingMetadata to fix typing issues
npx node-jq --slurpfile base scripts/spec-updates/BaseRatingMetadata.json '.components.schemas.BaseRatingMetadata = $base[0].BaseRatingMetadata' |
sed 's/"\$ref": "#\/components\/schemas\/AbstractRatingMetadata"/"\$ref": "#\/components\/schemas\/BaseRatingMetadata"/g' |

# Fix typing of TimeSeries response values
npx node-jq --slurpfile tsItems scripts/spec-updates/tsArrayItems.json '.components.schemas.TimeSeries.properties.values.items.items = $tsItems[0].items' |

# Remove uniqueItems designations (is bugged in generator)
npx node-jq 'del(.. | .uniqueItems?)' |

# Fix Office typing
npx node-jq '.paths.["/cwms-data/offices"].get.responses.["200"].content.[""] = .paths.["/cwms-data/offices"].get.responses.["200"].content.["application/json"]' |

# Change timeseries to time-series within schemas
npx node-jq '
    .components.schemas |= with_entries(
        .key |= gsub("timeseries"; "time-series") | 
        .value |= 
            if type == "string" then gsub("timeseries"; "time-series")
            elif type == "object" then walk(
                if type == "string" then gsub("timeseries"; "time-series") else . end
            )
        else . end
        )
    ' |

# Remove "CwmsData" from method names
sed -r 's/"(get|post|patch|put|delete)CwmsData(\w*)"/"\1\2"/g' |

# Change casing of "Timeseries" to "TimeSeries"
sed -r 's/Timeseries/TimeSeries/g' |

# Remove the /cwms-data base path on all endpoints so user can use their own
sed -r 's|"/cwms-data|"/|g' |
# Remove extra slashes
sed -r 's|"//|"/|g'  > cwms-swagger-mod.json |

# Write to file
cat > cwms-swagger-mod.json