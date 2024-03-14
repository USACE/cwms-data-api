package cwms.cda.formatters.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import cwms.cda.formatters.OutputFormatter;

/**
 * Inform the system of valid which Accept headers
 * use which OutputFormatter for a given data type.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Formattables.class)
public @interface FormattableWith {
    public String contentType();
    public Class<? extends OutputFormatter> formatter();
}