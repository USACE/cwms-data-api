package cwms.cda.data.dto.auth.users;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.junit.jupiter.api.Test;

class UserTest {
    private static final ContentType JSON = Formats.parseHeader(Formats.JSON, User.class);

    @Test
    void test_contact_fields_round_trip() throws Exception {
        User user = new User.Builder("TEST_USER", "test-principal", "alex.example@example.com", true)
                .firstName("Alex").lastName("Example").addRole("HQ", "CWMS Users").build();
        String serialized = Formats.format(JSON, user);
        JsonNode json = new ObjectMapper().readTree(serialized);
        assertEquals("Alex", json.get("first-name").asText());
        assertEquals("Example", json.get("last-name").asText());
        assertEquals("alex.example@example.com", json.get("email").asText());
        assertEquals("test-principal", json.get("principal").asText());
        User parsed = Formats.parseContent(JSON, serialized, User.class);
        assertEquals(user.getFirstName(), parsed.getFirstName());
        assertEquals(user.getLastName(), parsed.getLastName());
        assertEquals(user.getEmail(), parsed.getEmail());
        assertEquals(user.getRoles(), parsed.getRoles());
    }

    @Test
    void test_missing_contact_fields_are_omitted() throws Exception {
        User user = new User.Builder("TEST_USER", "test-principal", null, false).build();
        JsonNode json = new ObjectMapper().readTree(Formats.format(JSON, user));
        assertFalse(json.has("first-name"));
        assertFalse(json.has("last-name"));
        assertFalse(json.has("email"));
        assertEquals("TEST_USER", json.get("user-name").asText());
    }

    @Test
    void test_old_json_without_names_remains_readable() {
        User user = Formats.parseContent(JSON,
                "{\"user-name\":\"TEST_USER\",\"principal\":\"test-principal\","
                        + "\"email\":\"alex.example@example.com\",\"roles\":{}}", User.class);
        assertEquals("TEST_USER", user.getUserName());
        assertEquals("alex.example@example.com", user.getEmail());
        assertNull(user.getFirstName());
        assertNull(user.getLastName());
    }
}
