package fixtures.users;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import fixtures.RadarApiSetupCallback;
import fixtures.TestAccounts;
import fixtures.users.annotation.AuthType;
import fixtures.users.annotation.AuthType.UserType;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.Cookie;

public class UserSpecSource implements ArgumentsProvider {    
    /**
     * Get restassured request spec for 
     * setting appropriate credentials
     * @return
     */     
    public static ArrayList<Arguments> userSpecsValidPrivsWithGuest() {
        final ArrayList<Arguments> list = new ArrayList<>();
        list.add(Arguments.of("NONE",TestAccounts.KeyUser.GUEST,new RequestSpecBuilder().build()));
        Stream.of(TestAccounts.KeyUser.values())
              .filter(u -> u.getRoles().length > 0)
              .forEach(u -> {
                list.add(apiKeyUser(u));
                list.add(cwmsAaaUser(u));
              });
        return list;
    }

    public static ArrayList<Arguments> userSpecsValidPrivs() {
        final ArrayList<Arguments> list = new ArrayList<>();
        Stream.of(TestAccounts.KeyUser.values())
              .filter(u -> u.getRoles().length > 0)
              .forEach(u -> {
                list.add(apiKeyUser(u));
                list.add(cwmsAaaUser(u));
              });
        return list;
    }

    public static ArrayList<Arguments> usersNoPrivs() {
        final ArrayList<Arguments> list = new ArrayList<>();
        //list.add(Arguments.of("NONE",TestAccounts.KeyUser.GUEST,new RequestSpecBuilder().build()));
        Stream.of(TestAccounts.KeyUser.values())
              .filter(u -> u.getRoles().length == 0)
              .forEach(u -> {
                list.add(apiKeyUser(u));
                list.add(cwmsAaaUser(u));
              });
        return list;
    }

    private static Arguments apiKeyUser(TestAccounts.KeyUser user) {
        return Arguments.of("APIKEY",user,new RequestSpecBuilder().addHeader("Authorization",
                                       TestAccounts.KeyUser.SPK_NORMAL.toHeaderValue()).build());
    }

    private static Arguments cwmsAaaUser(TestAccounts.KeyUser user) {
        /**
         * For whatever reason our integration test tomcat system didn't
         * want to deal with JSESSIONIDSSO. For the purpose of these tests that 
         * doesn't matter.
         */ 
        Cookie cookie = new Cookie.Builder("JSESSIONID",user.getJSessionId())
                                  .setHttpOnly(true)
                                  .setDomain("localhost")
                                  .setSecured(true)
                                  .setSameSite("None")
                                  .setMaxAge(-1)
                                  .setPath("/")
                                  .build();
        return Arguments.of("JSESSION",user,new RequestSpecBuilder().addCookie(cookie).build());
    }

    private static Arguments jwtUser(TestAccounts.KeyUser user) {
        return Arguments.of();   
    }

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
        ArrayList<Arguments> args = new ArrayList<>();
        Optional<Method> testMethod = context.getTestMethod();
        if (testMethod.isPresent()) {
            Method method = testMethod.get();
            AuthType at = method.getAnnotation(AuthType.class);
            UserType uts[] = at.userTypes();
            for(UserType ut: uts) {
                if( ut.equals(UserType.GUEST_AND_PRIVS)) {
                    args.addAll(userSpecsValidPrivsWithGuest());
                } else if (ut.equals(UserType.NO_PRIVS)) {
                    args.addAll(usersNoPrivs());
                } else if (ut.equals(UserType.PRIVS)) {
                    args.addAll(userSpecsValidPrivsWithGuest());
                }
            }


            return args.stream();
        } else {
            throw new IllegalStateException("Accounts provider called from outside a test class");
        }
        
    }
}
