package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.stream.Bank;
import cwms.cda.data.dto.stream.StreamJunctionIdentifier;
import cwms.cda.data.dto.stream.StreamLocation;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class StreamLocationTest {

    @Test
    void createStreamLocation_allFieldsProvided_success() {
        LocationIdentifier streamLocationId = new LocationIdentifier.Builder()
                .withOfficeId("Office123")
                .withLocationId("StreamLoc123")
                .build();

        LocationIdentifier flowsIntoStreamId = new LocationIdentifier.Builder()
                .withOfficeId("Office123")
                .withLocationId("AnotherStream")
                .build();

        StreamJunctionIdentifier streamJunctionIdentifier = new StreamJunctionIdentifier.Builder()
                .withStreamId(flowsIntoStreamId)
                .withBank(Bank.LEFT)
                .withStation(123.45)
                .build();

        StreamLocation.Builder builder = new StreamLocation.Builder()
                .withStreamLocationId(streamLocationId)
                .withStreamJunctionId(streamJunctionIdentifier)
                .withPublishedStation(100.0)
                .withNavigationStation(90.0)
                .withLowestMeasurableStage(1.0)
                .withTotalDrainageArea(50.0)
                .withUngagedDrainageArea(20.0);

        StreamLocation item = builder.build();

        assertAll(() -> assertEquals(streamLocationId, item.getStreamLocationId(),
                        "The stream location id does not match the provided value"),
                () -> assertEquals(streamJunctionIdentifier, item.getStreamJunctionId(),
                        "The stream junction id does not match the provided value"),
                () -> assertEquals(100.0, item.getPublishedStation(), "The published station does not match the provided value"),
                () -> assertEquals(90.0, item.getNavigationStation(), "The navigation station does not match the provided value"),
                () -> assertEquals(1.0, item.getLowestMeasurableStage(), "The lowest measurable stage does not match the provided value"),
                () -> assertEquals(50.0, item.getTotalDrainageArea(), "The total drainage area does not match the provided value"),
                () -> assertEquals(20.0, item.getUngagedDrainageArea(), "The ungaged drainage area does not match the provided value"));
    }

    @Test
    void createStreamLocation_missingField_throwsFieldException() {
        assertThrows(FieldException.class, () -> {
            StreamLocation.Builder builder = new StreamLocation.Builder()
                    .withStreamJunctionId(new StreamJunctionIdentifier.Builder()
                            .withStreamId(new LocationIdentifier.Builder()
                                    .withOfficeId("Office123")
                                    .withLocationId("AnotherStream")
                                    .build())
                            .withBank(Bank.LEFT)
                            .withStation(123.45)
                            .build())
                    .withPublishedStation(100.0)
                    .withNavigationStation(90.0)
                    .withLowestMeasurableStage(1.0)
                    .withTotalDrainageArea(50.0)
                    .withUngagedDrainageArea(20.0);
            StreamLocation item = builder.build();
            item.validate();
        }, "The validate method should have thrown a FieldException because the stream junction id field is missing");
    }

    @Test
    void createStreamLocation_serialize_roundtrip() {
        LocationIdentifier locationIdentifier = new LocationIdentifier.Builder()
                .withOfficeId("Office123")
                .withLocationId("Stream123")
                .build();

        LocationIdentifier flowsIntoStreamId = new LocationIdentifier.Builder()
                .withOfficeId("Office123")
                .withLocationId("AnotherStream")
                .build();

        StreamJunctionIdentifier streamJunctionIdentifier = new StreamJunctionIdentifier.Builder()
                .withStreamId(flowsIntoStreamId)
                .withBank(Bank.LEFT)
                .withStation(123.45)
                .build();

        StreamLocation streamLocation = new StreamLocation.Builder()
                .withStreamLocationId(locationIdentifier)
                .withStreamJunctionId(streamJunctionIdentifier)
                .withPublishedStation(100.0)
                .withNavigationStation(90.0)
                .withLowestMeasurableStage(1.0)
                .withTotalDrainageArea(50.0)
                .withUngagedDrainageArea(20.0)
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, streamLocation);
        StreamLocation deserialized = Formats.parseContent(contentType, json, StreamLocation.class);
        assertEquals(streamLocation, deserialized, "StreamLocation deserialized from JSON doesn't equal original");
    }

    @Test
    void createStreamLocation_deserialize() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/stream-location.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        StreamLocation deserialized = Formats.parseContent(contentType, json, StreamLocation.class);
        assertNotNull(deserialized);
    }
}