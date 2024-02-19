package cwms.cda.helpers;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.function.Function;

public class ReplaceUtils {

    /**
     * Returns a Function that replaces occurrences of a key in a string template with a specified value.
     *
     * @param template the string template containing the key to be replaced
     * @param key the key to be replaced in the template
     * @return a Function that replaces occurrences of the key with a specified value
     */
    public static Function<String, String> replace(String template, String key){
        return value-> replace(template, key, value);
    }

    public static String replace(String template, String key, String value){
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
     */
    public static String replace(String template, String key, String value, boolean encode){
        try {
            if(encode){
                value = URLEncoder.encode(value, "UTF-8");
            }

            return template.replace(key,value );
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
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
    public static Function<String, String> alsoReplace(Function<String, String> mapper, String key, String value){
        return s-> replace(mapper.apply(s), key, value);
    }

    public static Function<String, String> alsoReplace(Function<String, String> mapper, String key, String value, boolean encode){
        return s-> replace(mapper.apply(s), key, value, encode);
    }



    public static void main(String[] args) {
        String template1= "http://localhost:7000/spk-data/clob/ignored?clob-id={clob-id}&office-id={office}";
        String template2 = "{context-root}clob/ignored?clob-id={clob-id}&office-id={office}";

        Function<String, String> mapper = replace(template1, "{clob-id}");
        mapper = alsoReplace(mapper, "{context-root}", "http://localhost:7000/spk-data/", false);
        mapper = alsoReplace(mapper, "{office}", "SWT");
        System.out.println(mapper.apply("/TIME SERIES TEXT/1978044"));

        mapper = replace(template2, "{clob-id}");
        mapper = alsoReplace(mapper, "{context-root}", "http://localhost:7000/spk-data/", false);
        mapper = alsoReplace(mapper, "{office}", "SWT");
        System.out.println(mapper.apply("/TIME SERIES TEXT/1978044"));

        System.out.println(replace("{context-path}", "{context-path}", "/spk-date"));
        System.out.println(replace("{context-path}", "{context-path}", "/spk-date", false));


    }
}
