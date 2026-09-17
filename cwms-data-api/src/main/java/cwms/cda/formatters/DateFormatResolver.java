package cwms.cda.formatters;


public final class DateFormatResolver {
    public static final String ISO_INSTANT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    public static final String ISO_OFFSET_PATTERN = "yyyy-MM-dd'T'HH:mm:ssXXX";
    public static final String ISO_LOCAL_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";
    public static final String DATE_ONLY_PATTERN = "yyyy-MM-dd";

    private DateFormatResolver() {
    }

    /**
     * Given either a named format or a custom parameter return an appropriate DateFormat instance.
     * At least one of the two cannot be null. If both are provide dateFormatParam takes
     * priority if valid.
     * @param dateFormatParam named format, can be null
     * @param customPattern custom date format pattern string, can be null.
     * @return DateFormat instance
     * @throws IllegalArgumentException if date pattern is null or "custom" and customPattern is null
     * @throws UnsupportedOperationException if no DateFormat instance can be created
     */
    public static DateFormat resolve(String dateFormatParam, String customPattern) {
        if (dateFormatParam == null || dateFormatParam.isEmpty()) {
            return DateFormat.pattern(ISO_INSTANT_PATTERN);
        }

        DateFormatParameter format = DateFormatParameter.get(dateFormatParam);
        if (format == null) {
            throw new UnsupportedOperationException("Unsupported date-format: " + dateFormatParam);
        }

        switch (format) {
            case EPOCH_MILLIS:
                return DateFormat.epochMillis();
            case ISO_INSTANT:
                return DateFormat.pattern(ISO_INSTANT_PATTERN);
            case ISO_OFFSET:
                return DateFormat.pattern(ISO_OFFSET_PATTERN);
            case ISO_LOCAL:
                return DateFormat.pattern(ISO_LOCAL_PATTERN);
            case DATE_ONLY:
                return DateFormat.pattern(DATE_ONLY_PATTERN);
            case CUSTOM:
                if (customPattern == null || customPattern.isEmpty()) {
                    throw new IllegalArgumentException(
                        "date-format-pattern is required when date-format is set to 'custom'");
                }
                return DateFormat.pattern(customPattern);
            default:
                throw new UnsupportedOperationException("Unsupported date-format: " + dateFormatParam);
        }
    }
}
