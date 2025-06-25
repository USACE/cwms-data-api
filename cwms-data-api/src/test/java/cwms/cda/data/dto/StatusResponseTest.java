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
        // all three fields provided
        StatusResponse item = new StatusResponse("TestOffice", "TestMessage", "TestIdentifier123");
        assertAll(() -> assertEquals("TestOffice", item.getOfficeId(), "The office ID does not match the provided value"),
                () -> assertEquals("TestMessage", item.getResponse(), "The response message does not match the provided value"),
                () -> assertEquals("TestIdentifier123", item.getIdentifier(), "The identifier does not match the provided value"));
        // only officeId and response message provided
        StatusResponse item2 = new StatusResponse("TestOffice2", "TestMessage2");
        assertAll(() -> assertEquals("TestOffice2", item2.getOfficeId(), "The office ID does not match the provided value"),
                () -> assertEquals("TestMessage2", item2.getResponse(), "The response message does not match the provided value"));
    }


    @Test
    void createStatusResponse_serialize_roundtrip() {
        StatusResponse statusResponse = new StatusResponse("Office123", "Message123", "Identifier123");
        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, statusResponse);
        StatusResponse deserialized = Formats.parseContent(contentType, json, StatusResponse.class);
        assertAll(
                () -> assertEquals(statusResponse.getOfficeId(), deserialized.getOfficeId(), "deserialized Office ID does not match provided value"),
                () -> assertEquals(statusResponse.getResponse(), deserialized.getResponse(), "deserialized response does not match provided value"),
                () -> assertEquals(statusResponse.getIdentifier(), deserialized.getIdentifier(), "deserialized identifier does not match provided value")
        );
    }

    @Test
    void createStatusResponse_deserialize_roundtrip() throws IOException {
        StatusResponse statusResponse = new StatusResponse("Office123", "Message123", "Identifier123");
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/status_response.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        StatusResponse deserialized = Formats.parseContent(contentType, json, StatusResponse.class);
        assertAll(
                () -> assertEquals(statusResponse.getOfficeId(), deserialized.getOfficeId(), "deserialized Office ID does not match provided value"),
                () -> assertEquals(statusResponse.getResponse(), deserialized.getResponse(), "deserialized response does not match provided value"),
                () -> assertEquals(statusResponse.getIdentifier(), deserialized.getIdentifier(), "deserialized identifier does not match provided value")
        );
    }
}
