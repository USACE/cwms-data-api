package cwms.cda.api.enums;

public enum Nation {
    US("UNITED STATES", "US"),
    CANADA("CANADA", "CA"),
    MEXICO("MEXICO", "MX");

    private final String name;
    private final String code;

    Nation(String name, String code) {
        this.name = name;
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public String getCode() {
        return code;
    }

    public static Nation nationForName(String nationName) {
        Nation retVal = null;
        for (Nation nation : values()) {
            if (nation.name.equalsIgnoreCase(nationName)) {
                retVal = nation;
                break;
            }
        }
        return retVal;
    }

    public static Nation nationForCode(String code) {
        Nation retVal = null;
        for (Nation nation : values()) {
            if (nation.code.equalsIgnoreCase(code)) {
                retVal = nation;
                break;
            }
        }
        return retVal;
    }
}
