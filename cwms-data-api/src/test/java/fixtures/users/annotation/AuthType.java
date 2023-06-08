package fixtures.users.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import fixtures.TestAccounts;

@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface AuthType {

    /**
     * The name of the static variable
     */
    UserType[] userTypes() default {};
    /**
     *
     * @return Specific user for test
     */
    TestAccounts.KeyUser user() default TestAccounts.KeyUser.NONE;

    public static enum UserType {
        GUEST_AND_PRIVS,
        PRIVS,
        NO_PRIVS
    }
}
