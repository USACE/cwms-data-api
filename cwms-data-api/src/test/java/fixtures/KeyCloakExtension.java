package fixtures;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import com.google.common.flogger.FluentLogger;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.images.builder.Transferable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.javalin.http.HttpCode;
import io.restassured.filter.log.LogDetail;
import io.restassured.http.ContentType;
import io.restassured.response.Response;

/**
 * Sets up a KeyCloak instance to use for testing.
 */
public final class KeyCloakExtension implements BeforeAllCallback {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    private static final String WELL_KNOWN = "realms/cwms/.well-known/openid-configuration";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final GenericContainer<?> kcc = new GenericContainer<>("quay.io/keycloak/keycloak:19.0.1")
                                                    .withEnv("KC_HTTP_ENABLED", "true")
                                                    .withEnv("KC_HOSTNAME_STRICT", "false")
                                                    .withEnv("KEYCLOAK_ADMIN","admin")
                                                    .withEnv("KEYCLOAK_ADMIN_PASSWORD","admin")
                                                    .withCommand("start-dev --features-disabled=admin2 --import-realm")
                                                    .withExposedPorts(8080)
                                                    .withReuse(false)
                                                    .withLogConsumer(frame -> 
                                                    {
                                                        logger.atInfo().log(frame.getUtf8String());
                                                    })
                                                    ;
                                                    
    private static String authUrl;
    private static String issuer;
    private static String codeUrl;
    private static String tokenUrl;
    

    
    static void setup() throws IOException {
        
        File realm = new File("../compose_files/keycloak/realm.json").getAbsoluteFile();
        String realmJson = null;
        try (FileInputStream is = new FileInputStream(realm)) {
            realmJson = IOUtils.toString(is, Charset.forName("UTF-8"));
        }
        kcc.withCopyToContainer(Transferable.of(realmJson), "/opt/keycloak/data/import/realm.json");
        kcc.setWaitStrategy(
            new LogMessageWaitStrategy().withRegEx("^.*Listening on:.*$")
                                        .withTimes(1)
                                        .withStartupTimeout(Duration.ofMinutes(5))
        );
        kcc.start();
        authUrl = "http://"+kcc.getHost()+":"+kcc.getMappedPort(8080);
        
        // verify we can get the wellknown config
        Response response = 
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .baseUri(authUrl)
            .when()
                .get(WELL_KNOWN);
        response
            .then()
            .statusCode(is(HttpCode.OK.getStatus()))
            ;
        
        JsonNode oidcConfig = mapper.readTree(response.asPrettyString());
        issuer = oidcConfig.get("issuer").asText();
        codeUrl = oidcConfig.get("authorization_endpoint").asText();
        tokenUrl = oidcConfig.get("token_endpoint").asText();
        logger.atFine().log(response.asPrettyString());
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (!kcc.isRunning()) {
            setup();
        }
    }

    public static String getAuthUrl() {
        return authUrl;
    }

    public static String getOidcWellKnown() {
        return authUrl+"/"+WELL_KNOWN;
    }

    public static String getIssuer() {
        return issuer;
    }

    public static String getCodeUrl() {
        return codeUrl;
    }

    public static String getTokenUrl() {
        return tokenUrl;
    }
    
    /**
     * Retrieve the Access token for the user.
     * @param username
     * @param password
     * @return Access token only
     */
    public static Optional<String> tokenForUser(String username, String password) {
        try {
            Response response = 
                given()
                    .log().ifValidationFails(LogDetail.ALL,true)
                    .contentType(ContentType.URLENC)
                    .formParam("client_id","cwms")
                    .formParam("grant_type","password")
                    .formParam("client_secret","")
                    .formParam("scope","openid profile email")
                    .formParam("username",username)
                    .formParam("password",password)
                    .formParam("response_type","token")
                .when()
                    .post(new URL(getTokenUrl()));
      
            logger.atFine().log(response.asPrettyString());
            JsonNode tokenInfo = mapper.readTree(response.asString());
            return Optional.of(tokenInfo.get("access_token").asText());
        } catch (JsonProcessingException | MalformedURLException ex) {
            logger.atWarning().withCause(ex).log("Unable to retrieve token for user %s", username);
            return Optional.empty();
        }
    }
}
