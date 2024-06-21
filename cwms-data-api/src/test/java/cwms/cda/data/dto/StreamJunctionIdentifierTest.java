package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.stream.Bank;
import cwms.cda.data.dto.stream.StreamJunctionIdentifier;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class StreamJunctionIdentifierTest {

    @Test
    void createStreamJunctionIdentifier_allFieldsProvided_success() {
        LocationIdentifier flowsIntoStreamId = new LocationIdentifier.Builder()
                .withOfficeId("Office123")
                .withName("AnotherStream")
                .build();

        StreamJunctionIdentifier.Builder builder = new StreamJunctionIdentifier.Builder()
                .withStreamId(flowsIntoStreamId)
                .withBank(Bank.LEFT)
                .withStation(123.45);

        StreamJunctionIdentifier item = builder.build();

        assertAll(() -> assertEquals(flowsIntoStreamId, item.getStreamId(),
                        "The flows into stream id does not match the provided value"),
                () -> assertEquals(Bank.LEFT, item.getBank(), "The bank does not match the provided value"),
                () -> assertEquals(123.45, item.getStation(), "The station does not match the provided value"));
    }

    @Test
    void createStreamJunctionIdentifier_missingField_throwsFieldException() {
        assertThrows(FieldException.class, () -> {
            StreamJunctionIdentifier.Builder builder = new StreamJunctionIdentifier.Builder()
                    .withBank(Bank.LEFT)
                    .withStation(123.45);
            StreamJunctionIdentifier item = builder.build();
            item.validate();
        }, "The validate method should have thrown a FieldException because the stream id field is missing");
    }

    @Test
    void createStreamJunctionIdentifier_serialize_roundtrip() {
        LocationIdentifier flowsIntoStreamId = new LocationIdentifier.Builder()
                .withOfficeId("Office123")
                .withName("AnotherStream")
                .build();

        StreamJunctionIdentifier streamJunctionIdentifier = new StreamJunctionIdentifier.Builder()
                .withStreamId(flowsIntoStreamId)
                .withBank(Bank.LEFT)
                .withStation(123.45)
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, streamJunctionIdentifier);
        StreamJunctionIdentifier deserialized = Formats.parseContent(contentType, json, StreamJunctionIdentifier.class);
        assertEquals(streamJunctionIdentifier, deserialized, "StreamJunctionIdentifier deserialized from JSON doesn't equal original");
    }

    @Test
    void createStreamJunctionIdentifier_deserialize() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/stream-junction-identifier.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        StreamJunctionIdentifier deserialized = Formats.parseContent(contentType, json, StreamJunctionIdentifier.class);
        assertNotNull(deserialized);
    }
}