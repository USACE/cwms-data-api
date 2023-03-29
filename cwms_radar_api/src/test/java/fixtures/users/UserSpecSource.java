package fixtures.users;

import java.util.ArrayList;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;

import fixtures.TestAccounts;
import io.restassured.builder.RequestSpecBuilder;

public class UserSpecSource {    
    /**
     * Get restassured request spec for 
     * setting appropriate credentials
     * @return
     */
    public static Stream<Arguments> userSpecsValidPrivsWithGuest() {
        final ArrayList<Arguments> list = new ArrayList<>();
        list.add(Arguments.of(TestAccounts.KeyUser.GUEST,new RequestSpecBuilder().build()));
        Stream.of(TestAccounts.KeyUser.values())
              .filter(u -> u.getRoles().length > 0)
              .forEach(u -> {
                list.add(apiKeyUser(u));
              });
        return list.stream();
    }

    public static Stream<Arguments> userSpecsValidPrivs() {
        final ArrayList<Arguments> list = new ArrayList<>();
        Stream.of(TestAccounts.KeyUser.values())
              .filter(u -> u.getRoles().length > 0)
              .forEach(u -> {
                list.add(apiKeyUser(u));
              });
        return list.stream();
    }

    private static Arguments apiKeyUser(TestAccounts.KeyUser user) {
        return Arguments.of(user,new RequestSpecBuilder().addHeader("Authorization",
                                       TestAccounts.KeyUser.SPK_NORMAL.toHeaderValue()).build());
    }
}
