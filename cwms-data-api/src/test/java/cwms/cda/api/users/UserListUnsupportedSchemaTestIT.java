package cwms.cda.api.users;

import static cwms.cda.api.auth.userlists.UserListFeature.UNSUPPORTED_MESSAGE;
import static cwms.cda.helpers.DatabaseHelpers.SCHEMA_VERSION.V2026_07_16;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import cwms.cda.api.DataApiTestIT;
import fixtures.KeyCloakExtension;
import fixtures.TestAccounts;
import fixtures.users.UserSpecSource;
import fixtures.users.annotation.AuthType;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag("integration")
@ExtendWith(KeyCloakExtension.class)
// TODO: Replace the assumption below with @MaximumSchema(260715) when that fixture exists.
public final class UserListUnsupportedSchemaTestIT extends DataApiTestIT {

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_NORMAL2)
    void test_user_lists_are_unsupported_on_older_schemas(String authType,
            TestAccounts.KeyUser user, RequestSpecification authSpec) {
        assumeTrue(getSchemaVersion() < V2026_07_16.numeric());

        given().spec(authSpec).queryParam("office", "SPK")
        .when().get("/user/list")
        .then().statusCode(501)
                .body("message", equalTo(UNSUPPORTED_MESSAGE));
    }
}
