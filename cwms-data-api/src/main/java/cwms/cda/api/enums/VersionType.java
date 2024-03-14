package cwms.cda.api.enums;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(
        name = "Version Type",
        description = "Version type specifies the type of timeseries response to be received. "
                + "Can be max aggregate or single version. Max aggregate cannot be run if version "
                + "date field is specified."
)
public enum VersionType {
    MAX_AGGREGATE,
    SINGLE_VERSION,
    UNVERSIONED;


    public static VersionType versionTypeFor(String versionType) {
        VersionType retval = null;

        if (versionType != null) {
            retval = VersionType.valueOf(versionType.toUpperCase());
        }
        return retval;
    }
}
