package cwms.cda.data.dao;

public enum StoreRule {
    // This class makes it obvious which of the constants in CWMS_UTIL_PACKAGE are store rules.
    // These constants used to be available from jooq'd CWMS_UTIL_PACKAGE via:
    //  usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE.REPLACE_ALL
    REPLACE_ALL("REPLACE ALL"),
    DO_NOT_REPLACE("DO NOT REPLACE"),
    REPLACE_MISSING_VALUES_ONLY("REPLACE MISSING VALUES ONLY"),
    REPLACE_WITH_NON_MISSING("REPLACE WITH NON MISSING"),
    DELETE_INSERT("DELETE INSERT");
    private final String rule;

    StoreRule(String rule) {
        String parts[] = rule.split("\\.");  // split on a literal period.
        this.rule = parts[parts.length - 1].replace("_", " ").replace("\"", "");
    }

    public String getRule() {
        return rule;
    }

    public static StoreRule getStoreRule(String input) {
        StoreRule retval = null;

        if (input != null) {
            input = input.replace(" ", "_");  // "REPLACE ALL" instead of "REPLACE_ALL"
            retval = StoreRule.valueOf(input.toUpperCase());
        }
        return retval;
    }

    @Override
    public String toString() {
        return rule;
    }

}
