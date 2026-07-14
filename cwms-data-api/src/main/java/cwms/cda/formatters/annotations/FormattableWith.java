package cwms.cda.formatters.annotations;

import cwms.cda.formatters.OutputFormatter;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Inform the system of valid which Accept headers
 * use which OutputFormatter for a given data type.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Formattables.class)
public @interface FormattableWith {
    /**
     * Content Type (MIME Type) string that maps to the given Formatter.
     * @return the configured content type.
     */
    public String contentType();
    /**
     * Which Formatter Class to use for the given ContentType.
     * @return the formatter Class instance.
     */
    public Class<? extends OutputFormatter> formatter();
    /**
     * Additional Content Type values, if any.
     * @return List of aliases, or zero length array.
     */
    String[] aliases() default {};
}