package cwms.cda.data.dao;

public enum VerticalDatum {
    NAVD88("NAVD88"),
    NGVD29("NGVD29"),
    NATIVE("NATIVE");

    private final String rule;

    VerticalDatum(String rule) {
        this.rule = rule;
    }

    public static VerticalDatum getVerticalDatum(String input) {
        VerticalDatum retval = null;

        if (input != null) {
            input = input.replace("-", "");
            retval = VerticalDatum.valueOf(input.toUpperCase());
        }
        return retval;
    }

    @Override
    public String toString() {
        return rule;
    }

}
