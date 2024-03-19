package cwms.cda.helpers;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.jetbrains.annotations.NotNull;

public class ReplaceUtils {

    private ReplaceUtils() {
        // Prevent instantiation
    }

    /**
     * Returns a Function that replaces occurrences of a key in a string template with a specified value.
     *
     * @param template the string template containing the key to be replaced
     * @param key the key to be replaced in the template
     * @return a Function that replaces occurrences of the key with a specified value
     */
    public static UnaryOperator<String> replace(@NotNull String template, @NotNull String key){
        return value-> replace(template, key, value);
    }

    public static String replace(@NotNull String template, @NotNull String key, String value){
        return replace(template, key, value, true);
    }

    /**
     * Replaces occurrences of a key in a string template with a specified value.
     *
     * @param template the string template containing the key to be replaced
     * @param key the key to be replaced in the template
     * @param value the value to replace the key with
     * @param encode true to URL encode the value, false otherwise
     * @return the modified string template with the key replaced by the value
     * @throws NullPointerException if the template is null
     */
    public static String replace(@NotNull String template, @NotNull String key, String value, boolean encode){
        String result = null;

        try {
            if (encode) {
                value = URLEncoder.encode(value, "UTF-8");
            }

            result = template.replace(key, value);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    /**
     * Returns a new Function that replaces occurrences of a key in a string with a specified value,
     * using the provided mapper function.
     *
     * @param mapper the mapper function to apply to the input string before replacing the key
     * @param key the key to be replaced in the string
     * @param value the value to replace the key with
     * @return a new Function that replaces occurrences of the key with the value
     */
    public static UnaryOperator<String> alsoReplace(UnaryOperator<String> mapper, String key, String value){
        return s-> replace(mapper.apply(s), key, value);
    }

    public static UnaryOperator<String> alsoReplace(UnaryOperator<String> mapper, String key, String value, boolean encode){
        return s-> replace(mapper.apply(s), key, value, encode);
    }


    public static class OperatorBuilder {

        String template;

        Map<String, String> replacements = new LinkedHashMap<>();
        private String operatorKey;

        public OperatorBuilder() {

        }

        public OperatorBuilder withTemplate(String template) {
            this.template = template;
            return this;
        }

        public OperatorBuilder replace(String key, String value) {
            return replace(key, value, true);
        }

        public OperatorBuilder replace(String key, String value, boolean encode) {
            if(value == null) {
                value = "";
            }
            if (encode) {
                try {
                    value = URLEncoder.encode(value, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
            replacements.put(key, value);
            return this;
        }

        public OperatorBuilder withOperatorKey(String key) {
            this.operatorKey = key;
            return this;
        }


        public UnaryOperator<String> build() {
            String result = template;

            // Apply the known replacements
            for (Map.Entry<String, String> entry : replacements.entrySet()) {
                result = result.replace(entry.getKey(), entry.getValue());
            }

            // Return the function that replaces the key with the result
            return ReplaceUtils.replace(result, operatorKey);
        }
    }

}
