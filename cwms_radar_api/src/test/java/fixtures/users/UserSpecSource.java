package fixtures.users;

import java.util.ArrayList;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;

import fixtures.RadarApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.Cookie;

public class UserSpecSource {    
    /**
     * Get restassured request spec for 
     * setting appropriate credentials
     * @return
     */     
    public static Stream<Arguments> userSpecsValidPrivsWithGuest() {
        final ArrayList<Arguments> list = new ArrayList<>();
        list.add(Arguments.of("NONE",TestAccounts.KeyUser.GUEST,new RequestSpecBuilder().build()));
        Stream.of(TestAccounts.KeyUser.values())
              .filter(u -> u.getRoles().length > 0)
              .forEach(u -> {
                list.add(apiKeyUser(u));
                list.add(cwmsAaaUser(u));
              });
        return list.stream();
    }

    public static Stream<Arguments> userSpecsValidPrivs() {
        final ArrayList<Arguments> list = new ArrayList<>();
        Stream.of(TestAccounts.KeyUser.values())
              .filter(u -> u.getRoles().length > 0)
              .forEach(u -> {
                list.add(apiKeyUser(u));
                list.add(cwmsAaaUser(u));
              });
        return list.stream();
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
}
