package cwms.cda.helpers;


public class DatabaseHelpers {

    public static final int LATEST_SCHEMA = 999999;
    
    public enum SCHEMA_VERSION {
        V2025_07_01(250701, "25.07.01"),
        LATEST_DEV(LATEST_SCHEMA, "99.99.99"),
        BYPASS(-1, "Bypass")
        ;

        private final int numeric;
        private final String text;

        SCHEMA_VERSION(int numeric, String text) {
            this.numeric = numeric;
            this.text = text;
        }

        public int numeric() {
            return this.numeric;
        }

        public String text() {
            return this.text;
        }

        /**
         * Return Schema enum constant from provided database version integer.
         * @param value the integer representation of the database schema version.
         * @return the appropriate Enum
         * @throws IllegalArgumentException if the value cannot be mapped.
         */
        public static SCHEMA_VERSION fromNumeric(int value) {
            for (var tmp: SCHEMA_VERSION.values()) {
                if (tmp.numeric == value) {
                    return tmp;
                }
            }
            throw new IllegalArgumentException(
                "Numeric Value " + value + " does not match an available version enumeration.");
        }
    }
}
