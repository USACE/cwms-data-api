package cwms.cda.api.auth;

import static io.javalin.apibuilder.ApiBuilder.crud;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.plugin.openapi.OpenApiOptions;
import io.javalin.plugin.openapi.OpenApiPlugin;
import io.swagger.v3.oas.models.info.Info;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ApiKeyOpenApiTest {
    @Test
    void getSchemasExcludeSecretAndCreationDocumentsOneTimeSecret() throws Exception {
        Javalin app = Javalin.create(config -> config.registerPlugin(new OpenApiPlugin(
                new OpenApiOptions(new Info().title("API keys").version("test")).path("/swagger-docs"))));
        try {
            app.routes(() -> crud("/auth/keys/{key-name}", new ApiKeyController(new MetricRegistry())));
            app.start(0);
            HttpResponse<String> response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                    URI.create("http://localhost:" + app.port() + "/swagger-docs")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            JsonNode spec = new ObjectMapper().readTree(response.body());
            JsonNode paths = spec.get("paths");
            JsonNode list = paths.get("/auth/keys").get("get");
            JsonNode get = paths.get("/auth/keys/{key-name}").get("get");
            JsonNode post = paths.get("/auth/keys").get("post");
            String responseSchema = "/responses/200/content/application~1json/schema";
            assertEquals("#/components/schemas/ApiKeyMetadata", get.at(responseSchema + "/$ref").asText());
            assertEquals("#/components/schemas/ApiKeyMetadata", list.at(responseSchema + "/items/$ref").asText());
            assertEquals("#/components/schemas/ApiKey",
                    post.at("/responses/201/content/application~1json/schema/$ref").asText());
            assertFalse(spec.at("/components/schemas/ApiKeyMetadata/properties").has("api-key"));
            assertTrue(spec.at("/components/schemas/ApiKey/properties").has("api-key"));
            for (JsonNode operation : new JsonNode[]{list, get, post}) {
                assertTrue(operation.get("description").asText().contains("**Save your API key securely"));
                assertTrue(operation.get("description").asText().contains("It cannot be retrieved again."));
            }
            Files.writeString(Path.of("build/api-key-openapi.json"), response.body());
        } finally {
            app.stop();
        }
    }
}
