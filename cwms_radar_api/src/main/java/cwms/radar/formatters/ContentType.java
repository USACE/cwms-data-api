package cwms.radar.formatters;

import com.google.common.flogger.FluentLogger;
import java.util.LinkedHashMap;
import java.util.Map;

public class ContentType implements Comparable<ContentType> {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String PARAM_DELIM = ";";
    public static final String ELEM_DELIM = "=";
    public static final String CHARSET = "charset";
    private String mediaType;
    private Map<String, String> parameters;

    private String charset = null;

    public ContentType(String contentTypeHeader) {
        parameters = new LinkedHashMap<>();
        String[] parts = contentTypeHeader.split(PARAM_DELIM);
        mediaType = parts[0];
        if (parts.length > 1) {
            for (int i = 1; i < parts.length; i++) {
                String[] key_val = parts[i].split(ELEM_DELIM);
                if (key_val.length == 2) {
                    String key = key_val[0].trim();
                    String value = key_val[1].trim();
                    if (CHARSET.equalsIgnoreCase(key)) {
                        charset = value;
                    } else {
                        parameters.put(key, value);
                    }
                }
            }
        }
    }

    public String getType() {
        return mediaType;
    }

    public Map<String, String> getParameters() {
        return new LinkedHashMap<>(parameters);
    }

    public String getCharset() {
        return charset;
    }

    @Override
    public boolean equals(Object other) {
        logger.atFinest().log("Checking %s vs %s", this, other);
        if (!(other instanceof ContentType)) return false;
        ContentType o = (ContentType) other;
        if (!(mediaType.equals(o.mediaType))) return false;

        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String key = entry.getKey();
            if (key.equals("q")) continue; // we don't care about q for equals
            if (!entry.getValue().equals(o.parameters.get(key))) return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public int compareTo(ContentType o) {
        float myPriority = Float.parseFloat(parameters.getOrDefault("q", "1"));
        float otherPriority = Float.parseFloat(parameters.getOrDefault("q", "1"));
        if (myPriority == otherPriority) return 0;
        else if (myPriority > otherPriority) return 1;
        else return -1;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(mediaType);

        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String key = entry.getKey();
            if (key.equals("q")) continue;
            builder.append(PARAM_DELIM).append(key).append(ELEM_DELIM).append(entry.getValue());
        }

        return builder.toString();
    }
}
