package fixtures.users.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface AuthType {

    /**
     * The name of the static variable
     */
    UserType[] userTypes();

    public static enum UserType {
        GUEST_AND_PRIVS,
        PRIVS,
        NO_PRIVS
    }
}
