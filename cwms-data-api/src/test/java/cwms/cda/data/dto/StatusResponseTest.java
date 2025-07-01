package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

final class StatusResponseTest {

    @Test
    void createStatusResponse_allFieldsProvided_Success() {
        // both fields provided
        StatusResponse item = new StatusResponse("Created Location Level", "TestIdentifier123");
        assertAll(() -> assertEquals("Created Location Level", item.getMessage(), "The response message does not match the provided value"),
                () -> assertEquals("TestIdentifier123", item.getIdentifier(), "The identifier does not match the provided value"));
        // only message provided
        StatusResponse item2 = new StatusResponse("Updated Location");
        assertAll(() -> assertEquals("Updated Location", item2.getMessage(), "The response message does not match the provided value"));
    }


    @Test
    void createStatusResponse_serialize_roundtrip() {
        StatusResponse statusResponse = new StatusResponse("Created Location Level", "Identifier123");
        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, statusResponse);
        StatusResponse deserialized = Formats.parseContent(contentType, json, StatusResponse.class);
        assertAll(
                () -> assertEquals(statusResponse.getMessage(), deserialized.getMessage(), "deserialized response does not match provided value"),
                () -> assertEquals(statusResponse.getIdentifier(), deserialized.getIdentifier(), "deserialized identifier does not match provided value")
        );
    }

    @Test
    void createStatusResponse_deserialize_roundtrip() throws IOException {
        StatusResponse statusResponse = new StatusResponse("Created Location Level", "Identifier123");
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/status_response.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        StatusResponse deserialized = Formats.parseContent(contentType, json, StatusResponse.class);
        assertAll(
                () -> assertEquals(statusResponse.getMessage(), deserialized.getMessage(), "deserialized response does not match provided value"),
                () -> assertEquals(statusResponse.getIdentifier(), deserialized.getIdentifier(), "deserialized identifier does not match provided value")
        );
    }
}
