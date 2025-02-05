package cwms.cda.api.auth;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.fail;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

import com.github.dockerjava.api.model.Bind;

import cwms.cda.api.DataApiTestIT;
import io.javalin.http.HttpCode;
import io.restassured.filter.log.LogDetail;
import io.restassured.http.ContentType;

@Tag("integration")
public class OpenIdConnectTestIT { //} extends DataApiTestIT {
    private static final Logger logger = Logger.getLogger(OpenIdConnectTestIT.class.getName());
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
                                                        logger.info(frame.getUtf8String());
                                                    })
                                                    ;
                                                    
    private static String authUrl;

    @BeforeAll
    static void setup() throws IOException {
        File realm = new File("../compose_files/keycloak/realm.json").getAbsoluteFile();
        String realmJson = null;
        try (FileInputStream is = new FileInputStream(realm)){
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
        Map<String,String> request = new HashMap<>();
        

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .baseUri(authUrl)
        .when()
            .get("realms/cwms/.well-known/openid-configuration")
        .then()
            .statusCode(is(HttpCode.OK.getStatus()));

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .baseUri(authUrl)
            .contentType(ContentType.URLENC)
            .formParam("client_id","cwms")
            .formParam("grant_type","password")
            .formParam("client_secret","")
            .formParam("scope","openid profile email")
            .formParam("username","m5hectest")
            .formParam("password","m5hectest")
            .formParam("response_type","token")
        .when()
            .post("realms/cwms/protocol/openid-connect/token")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.OK.getStatus()))
            ;
    }

    @Test
    void test_keycloak_user_is_created() {
        System.out.println(authUrl);
        fail();
    }
}
