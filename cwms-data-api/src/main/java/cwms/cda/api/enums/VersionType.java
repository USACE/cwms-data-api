package cwms.cda.api.enums;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(
        name = "Version Type",
        description = "Version type specifies the type of timeseries response to be received. Can be max aggregate "
                + "or single version. Max aggregate cannot be ran if version date field is specified."
)
public enum VersionType {
    MAX_AGGREGATE,
    SINGLE_VERSION,
    UNVERSIONED,
    UNDEFINED;

    public static final String DESCRIPTION = "Version type specifies the type of timeseries response to be received. Can be max aggregate "
            + "or single version. Max aggregate cannot be ran if version date field is specified. If "
            + "unspecified, defaults to max aggregate.";

    public static VersionType versionTypeFor(String versionType) {
        VersionType retval = UNDEFINED;

        if (versionType != null) {
            retval = VersionType.valueOf(versionType.toUpperCase());
        }
        return retval;
    }
}
