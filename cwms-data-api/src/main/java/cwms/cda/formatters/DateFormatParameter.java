package cwms.cda.formatters;

public enum DateFormatParameter {
    EPOCH_MILLIS("epoch-millis"),
    ISO_INSTANT("ISO8601-Instant"),
    ISO_OFFSET("ISO8601-Offset"),
    ISO_LOCAL("ISO8601-Local"),
    DATE_ONLY("date-only"),
    CUSTOM("custom");

    private final String value;

    DateFormatParameter(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static DateFormatParameter get(String value) {
        for (DateFormatParameter format : DateFormatParameter.values()) {
            if (format.value.equalsIgnoreCase(value)) {
                return format;
            }
        }
        return null;
    }
}
