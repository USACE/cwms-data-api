package cwms.radar.formatters;

import com.google.common.flogger.FluentLogger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

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

    /**
     * For the purposes of cwms-data-api content-type equals we only care about the following
     * fields matching:
     * 
     *  - the mimetype itself
     *  - the version parameter
     * 
     * For us everything else is informational or used indirectly
     */
    @Override
    public boolean equals(Object other) {
        logger.atFinest().log("Checking %s vs %s", this, other);
        if (!(other instanceof ContentType)) return false;
        ContentType o = (ContentType) other;
        if (!(mediaType.equals(o.mediaType))) return false;

        /** We loop through instead of using contains key. 
         *  Content-type parameter names are not case sensitive.
         */
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String key = entry.getKey();
            if( "version".equalsIgnoreCase(key) ) {
                return entry.getValue().equals(o.parameters.get(key));
            }
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

    /**
     * Used for quick comparisons where we don't further need the content type.
     * @param a
     * @param b
     * @return whether they are equivalent
     */
    final public static boolean equivalent(String a, String b) {
        Objects.requireNonNull(a, "Cannot determine equivalency of null content-types");
        Objects.requireNonNull(b, "Cannot determine equivalency of null content-types");
        ContentType ctA = new ContentType(a);
        ContentType ctB = new ContentType(b);
        return ctA.equals(ctB);
    }
}
