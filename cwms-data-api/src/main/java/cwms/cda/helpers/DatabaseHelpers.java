package cwms.cda.helpers;


public class DatabaseHelpers {

    
    public static enum SCHEMA_VERSION {
        V2025_07_01(250701, "25.07.01"),
        LATEST_DEV(999999, "99.99.99"),
        BYPASS(-1, "Bypass")
        ;

        private final int numeric;
        private final String text;

        SCHEMA_VERSION(int numeric, String text)
        {
            this.numeric = numeric;
            this.text = text;
        }

        public int numeric() {
            return this.numeric;
        }

        public String text() {
            return this.text;
        }

        public static SCHEMA_VERSION fromNumeric(int value)
        {
            for(var tmp: SCHEMA_VERSION.values())
            {
                if (tmp.numeric == value)
                {
                    return tmp;
                }
            }
            throw new IllegalArgumentException(
                "Numeric Value " + value + " does not match an available version enumeration.");
        }
    }
}
