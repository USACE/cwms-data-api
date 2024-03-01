package cwms.cda.data.dao.texttimeseries;

public enum TimeSeriesTextMode {
    ALL,
    STANDARD,
    REGULAR;

    public static TimeSeriesTextMode getMode(String input) {
        TimeSeriesTextMode retval = null;

        if (input != null) {
            retval = TimeSeriesTextMode.valueOf(input.toUpperCase());
        }
        return retval;
    }

}
