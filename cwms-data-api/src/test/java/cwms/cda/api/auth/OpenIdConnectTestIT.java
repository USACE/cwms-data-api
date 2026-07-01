package cwms.cda.api.auth;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.api.DataApiTestIT;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.KeyCloakExtension;
import io.javalin.http.HttpCode;
import io.restassured.filter.log.LogDetail;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Tag("integration")
@ExtendWith(KeyCloakExtension.class)
public class OpenIdConnectTestIT extends DataApiTestIT {
    private static final String SWT_BATCH_CLIENT = "cwms-batch-runner-swt";
    private static final String SWT_BATCH_CLIENT_SECRET = "local-cwms-batch-runner-swt-secret";
    private static final String SWT_BATCH_USER = "SERVICE-ACCOUNT-CWMS-BATCH-RUNNER-SWT";
    private static final String SPK_BATCH_CLIENT = "cwms-batch-runner-spk";
    private static final String SPK_BATCH_CLIENT_SECRET = "local-cwms-batch-runner-spk-secret";
    private static final String SPK_BATCH_USER = "SERVICE-ACCOUNT-CWMS-BATCH-RUNNER-SPK";

    @Test
    void test_keycloak_user_is_created() {

        Optional<String> token = KeyCloakExtension.tokenForUser("q0hecoidc", "q0hecoidc");
        assertTrue(token.isPresent());

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .header("Authorization", "Bearer " + token.get())
            .queryParam("name-mask","asdf")
        .when()
            .get("/properties")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            // 403 Forbidden here means the user was created, but has no privileges as the user was just created.
            // Which is what we desire to happen in this test.
            .statusCode(is(HttpCode.FORBIDDEN.getStatus()));
    }

    @Test
    void test_keycloak_user_can_operate() {
        Optional<String> token = KeyCloakExtension.tokenForUser("m5hectest", "m5hectest");
        assertTrue(token.isPresent());

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .header("Authorization", "Bearer " + token.get())
            .queryParam("name-mask","asdf")
        .when()
            .get("/properties")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.OK.getStatus()));
    }

    @Test
    void test_keycloak_batch_service_account_claims_require_registered_machine_principal() throws Exception {
        Optional<String> spkToken = KeyCloakExtension.tokenForClientCredentials(SPK_BATCH_CLIENT,
            SPK_BATCH_CLIENT_SECRET);
        assertTrue(spkToken.isPresent());
        assertBatchClaims(spkToken.get(), "SPK", SPK_BATCH_CLIENT);

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .header("Authorization", "Bearer " + spkToken.get())
        .when()
            .get("/user/profile")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.UNAUTHORIZED.getStatus()));

        registerBatchMachinePrincipal(SPK_BATCH_USER, spkToken.get(), "SPK");

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .header("Authorization", "Bearer " + spkToken.get())
        .when()
            .get("/user/profile")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.OK.getStatus()))
            .body("user-name", equalTo(SPK_BATCH_USER))
            .body("roles.SPK", hasItems("All Users", "CWMS Users", "TS ID Creator"))
            .body("roles.SWT", nullValue());

        Optional<String> swtToken = KeyCloakExtension.tokenForClientCredentials(SWT_BATCH_CLIENT,
            SWT_BATCH_CLIENT_SECRET);
        assertTrue(swtToken.isPresent());
        assertBatchClaims(swtToken.get(), "SWT", SWT_BATCH_CLIENT);
        registerBatchMachinePrincipal(SWT_BATCH_USER, swtToken.get(), "SWT");

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .header("Authorization", "Bearer " + swtToken.get())
        .when()
            .get("/user/profile")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.OK.getStatus()))
            .body("user-name", equalTo(SWT_BATCH_USER))
            .body("roles.SWT", hasItems("All Users", "CWMS Users", "TS ID Creator"))
            .body("roles.SPK", nullValue());
    }

    private static void assertBatchClaims(String token, String office, String clientId) throws Exception {
        assertEquals(true, KeyCloakExtension.claims(token).get("machine_auth").asBoolean());
        assertEquals(office, KeyCloakExtension.claims(token).get("run_as_office").asText());
        assertEquals("service-account-" + clientId,
            KeyCloakExtension.claims(token).get("preferred_username").asText());
    }

    private static void registerBatchMachinePrincipal(String userName, String token, String office) throws Exception {
        addNewUser(userName);
        updateOidcPrincipal(userName, token);
        addUserToGroup(userName, "CWMS Users", office);
        addUserToGroup(userName, "All Users", office);
        addUserToGroup(userName, "TS ID Creator", office);
    }

    private static void updateOidcPrincipal(String userName, String token) throws Exception {
        String oidcPrincipal = KeyCloakExtension.getIssuer() + "::"
            + KeyCloakExtension.claims(token).get("sub").asText();
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection((c) -> {
            try (PreparedStatement stmt = c.prepareStatement(
                "update AT_SEC_CWMS_USERS set principle_name = ? where userid = upper(?)")) {
                stmt.setString(1, oidcPrincipal);
                stmt.setString(2, userName);
                stmt.executeUpdate();
            } catch (SQLException ex) {
                throw new RuntimeException("Unable to update OIDC principal for: " + userName, ex);
            }
        }, "cwms_20");
    }
}
