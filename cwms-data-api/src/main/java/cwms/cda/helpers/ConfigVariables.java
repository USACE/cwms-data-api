package cwms.cda.helpers;

public final class ConfigVariables {
    private ConfigVariables() {
        /* utility class */
    }

    public static String getConfigString(String name) {
        return getConfigString(name, null);
    }

    public static String getConfigString(String name, String defaultValue) {
        String ret = System.getenv(name);
        if (ret == null) {
            ret = System.getProperty(name, defaultValue);
        }
        return ret;
    }

    public static int getConfigInt(String name, int defaultValue) {
        var stringValue = getConfigString(name);
        int ret = defaultValue;
        if (stringValue != null && !stringValue.isBlank()) {
            ret = Integer.parseInt(stringValue);
        }
        return ret;
    }
}
