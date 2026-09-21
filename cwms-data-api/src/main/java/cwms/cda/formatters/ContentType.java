package cwms.cda.formatters;

import com.google.common.flogger.FluentLogger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Stores an instance of ContentType and it's parameters.
 * Example <pre>application/json;q=1</pre> is different than <pre>application/json;q=2</pre>
 */
public class ContentType implements Comparable<ContentType> {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String PARAM_DELIM = ";";
    public static final String ELEM_DELIM = "=";
    public static final String CHARSET = "charset";
    private final String mediaType;
    private final Map<String, String> parameters;

    private String instanceCharset = null;

    /**
     * Create a ContentType instance given the provide ContentType or Accept Header.
     * The contructor will parse query parameters out of the provided string.
     * @param contentTypeHeader provided ContentType or Accept header text
     */
    public ContentType(String contentTypeHeader) {
        parameters = new LinkedHashMap<>();
        String[] parts = contentTypeHeader.split(PARAM_DELIM);
        mediaType = parts[0];
        if (parts.length > 1) {
            for (int i = 1; i < parts.length; i++) {
                String[] keyVal = parts[i].split(ELEM_DELIM);
                if (keyVal.length == 2) {
                    String key = keyVal[0].trim();
                    String value = keyVal[1].trim();
                    if (CHARSET.equalsIgnoreCase(key)) {
                        instanceCharset = value;
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

    public String getInstanceCharset() {
        return instanceCharset;
    }

    /**
     * For the purposes of cwms-data-api content-type equals we only care about the following
     * fields matching.
     *
     * <p>
     *  - the mimetype itself
     *  - the version parameter
     *
     * <p>
     * For us everything else is informational or used indirectly
     */
    @Override
    public boolean equals(Object other) {
        logger.atFinest().log("Checking %s vs %s", this, other);
        if (!(other instanceof ContentType)) {
            return false;
        }
        ContentType o = (ContentType) other;
        if (!(mediaType.equals(o.mediaType))) {
            return false;
        }

        /* We loop through instead of using contains key.
         *  Content-type parameter names are not case sensitive.
         */
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String key = entry.getKey();
            if ("version".equalsIgnoreCase(key)) {
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
    public int compareTo(ContentType other) {
        float myPriority = Float.parseFloat(parameters.getOrDefault("q", "1"));
        float otherPriority = Float.parseFloat(other.parameters.getOrDefault("q", "1"));
        if (myPriority == otherPriority) {
            return 0;
        } else if (myPriority > otherPriority) {
            return 1;
        } else {
            return -1;
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(mediaType);

        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String key = entry.getKey();
            if (key.equals("q")) {
                continue;
            }
            builder.append(PARAM_DELIM).append(key).append(ELEM_DELIM).append(entry.getValue());
        }

        return builder.toString();
    }

    /**
     * Used for quick comparisons where we don't further need the content type
     * so we can streamline the code a little.
     *
     * @param a first content type to check
     * @param b second content type to check
     * @return whether they are equivalent
     */
    public static boolean equivalent(String a, String b) {
        Objects.requireNonNull(a, "Cannot determine equivalency of null content-types");
        Objects.requireNonNull(b, "Cannot determine equivalency of null content-types");
        ContentType ctA = new ContentType(a);
        ContentType ctB = new ContentType(b);
        return ctA.equals(ctB);
    }
}
