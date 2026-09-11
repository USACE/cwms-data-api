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
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.models.info.Info;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ApiKeyOpenApiTest {
    @Test
    void getSchemasExcludeSecretAndCreationDocumentsOneTimeSecret() throws Exception {
        OpenApiPlugin plugin = new OpenApiPlugin(
                new OpenApiOptions(new Info().title("API keys").version("test")).path("/swagger-docs"));
        Javalin app = Javalin.createStandalone(config -> config.registerPlugin(plugin));
        app.routes(() -> crud("/auth/keys/{key-name}", new ApiKeyController(new MetricRegistry())));
        String json = Json.mapper().writeValueAsString(plugin.getOpenApiHandler().createOpenAPISchema());
        JsonNode spec = new ObjectMapper().readTree(json);
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
        Files.writeString(Path.of("build/api-key-openapi.json"), json);
    }
}
