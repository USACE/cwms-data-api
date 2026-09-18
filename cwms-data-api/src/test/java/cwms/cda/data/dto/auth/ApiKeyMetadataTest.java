package cwms.cda.data.dto.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.plugin.json.JavalinJackson;
import java.time.ZonedDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class ApiKeyMetadataTest {
    private final JavalinJackson json = new JavalinJackson();
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void retrievalOmitsSecretAndPreservesMetadata() throws Exception {
        ApiKey key = new ApiKey("user", "script", "creation-only-secret",
                ZonedDateTime.parse("2026-09-11T12:00:00Z"), null);
        ApiKeyMetadata metadata = new ApiKeyMetadata("user", "script", key.getCreated(), null);
        JsonNode created = mapper.readTree(json.toJsonString(key));
        JsonNode retrieved = mapper.readTree(json.toJsonString(metadata));
        assertEquals("creation-only-secret", created.get("api-key").asText());
        assertFalse(retrieved.has("api-key"));
        assertEquals(4, retrieved.size());
        for (String field : List.of("user-id", "key-name", "created", "expires")) {
            assertEquals(created.get(field), retrieved.get(field), field);
        }
        assertTrue(retrieved.get("expires").isNull());
        JsonNode listed = mapper.readTree(json.toJsonString(List.of(metadata)));
        assertEquals(retrieved, listed.get(0));
    }

    @Test
    void retrievalPreservesExpirationWithoutNullSecret() throws Exception {
        ZonedDateTime expires = ZonedDateTime.parse("2026-12-11T12:00:00Z");
        ApiKey key = new ApiKey("user", "script", null, expires.minusDays(90), expires);
        JsonNode original = mapper.readTree(json.toJsonString(key));
        ApiKeyMetadata metadata = new ApiKeyMetadata("user", "script", expires.minusDays(90), expires);
        JsonNode retrieved = mapper.readTree(json.toJsonString(metadata));
        assertFalse(retrieved.has("api-key"));
        assertEquals(original.get("expires"), retrieved.get("expires"));
    }
}
