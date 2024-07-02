/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including but not limited to the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.stream.Bank;
import cwms.cda.data.dto.stream.StreamNode;
import cwms.cda.data.dto.stream.StreamLocation;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import org.apache.commons.io.IOUtils;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

final class StreamLocationTest {

    @Test
    void createStreamLocation_allFieldsProvided_success() {
        CwmsId streamLocationId = new CwmsId.Builder()
                .withOfficeId("SPK")
                .withName("StreamLoc123")
                .build();

        CwmsId flowsIntoStreamId = new CwmsId.Builder()
                .withOfficeId("SPK")
                .withName("AnotherStream")
                .build();

        StreamNode streamNode = new StreamNode.Builder()
                .withStreamId(flowsIntoStreamId)
                .withBank(Bank.LEFT)
                .withStation(123.45)
                .withStationUnits("ft")
                .build();

        StreamLocation.Builder builder = new StreamLocation.Builder()
                .withId(streamLocationId)
                .withStreamNode(streamNode)
                .withPublishedStation(100.0)
                .withNavigationStation(90.0)
                .withLowestMeasurableStage(1.0)
                .withTotalDrainageArea(50.0)
                .withUngagedDrainageArea(20.0)
                .withAreaUnits("mi2")
                .withStageUnits("ft");

        StreamLocation item = builder.build();

        assertAll(() -> {
                    assertEquals(streamLocationId.getName(), item.getId().getName(),
                            "The stream location id name does not match the provided value");
                    assertEquals(streamLocationId.getOfficeId(), item.getId().getOfficeId(),
                            "The stream location id officeId does not match the provided value");
                },
                () -> {
                    assertEquals(flowsIntoStreamId.getName(), item.getStreamNode().getStreamId().getName(),
                            "The flows into stream id name does not match the provided value");
                    assertEquals(flowsIntoStreamId.getOfficeId(), item.getStreamNode().getStreamId().getOfficeId(),
                            "The flows into stream id officeId does not match the provided value");
                },
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
                    .withStreamNode(new StreamNode.Builder()
                            .withStreamId(new CwmsId.Builder()
                                    .withOfficeId("SPK")
                                    .withName("AnotherStream")
                                    .build())
                            .withBank(Bank.LEFT)
                            .withStation(123.45)
                            .withStationUnits("ft")
                            .build())
                    .withPublishedStation(100.0)
                    .withNavigationStation(90.0)
                    .withLowestMeasurableStage(1.0)
                    .withTotalDrainageArea(50.0)
                    .withUngagedDrainageArea(20.0)
                    .withAreaUnits("mi2")
                    .withStageUnits("ft");
            StreamLocation item = builder.build();
            item.validate();
        }, "The validate method should have thrown a FieldException because the stream location id field is missing");

        assertThrows(FieldException.class, () -> {
            StreamLocation.Builder builder = new StreamLocation.Builder()
                    .withId(new CwmsId.Builder()
                            .withOfficeId("SPK")
                            .withName("StreamLoc123")
                            .build())
                    .withPublishedStation(100.0)
                    .withNavigationStation(90.0)
                    .withLowestMeasurableStage(1.0)
                    .withTotalDrainageArea(50.0)
                    .withUngagedDrainageArea(20.0)
                    .withAreaUnits("mi2")
                    .withStageUnits("ft");
            StreamLocation item = builder.build();
            item.validate();
        }, "The validate method should have thrown a FieldException because the stream node field is missing");
    }

    @Test
    void createStreamLocation_serialize_roundtrip() {
        CwmsId cwmsId = new CwmsId.Builder()
                .withOfficeId("SPK")
                .withName("Stream123")
                .build();

        CwmsId flowsIntoStreamId = new CwmsId.Builder()
                .withOfficeId("SPK")
                .withName("AnotherStream")
                .build();

        StreamNode streamNode = new StreamNode.Builder()
                .withStreamId(flowsIntoStreamId)
                .withBank(Bank.LEFT)
                .withStation(123.45)
                .withStationUnits("ft")
                .build();

        StreamLocation streamLocation = new StreamLocation.Builder()
                .withId(cwmsId)
                .withStreamNode(streamNode)
                .withPublishedStation(100.0)
                .withNavigationStation(90.0)
                .withLowestMeasurableStage(1.0)
                .withTotalDrainageArea(50.0)
                .withUngagedDrainageArea(20.0)
                .withAreaUnits("mi2")
                .withStageUnits("ft")
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, streamLocation);
        StreamLocation deserialized = Formats.parseContent(contentType, json, StreamLocation.class);
        DTOMatch.assertMatch(streamLocation, deserialized);
    }

    @Test
    void createStreamLocation_deserialize() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/stream_location.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        StreamLocation deserialized = Formats.parseContent(contentType, json, StreamLocation.class);

        assertAll(
                () -> assertEquals("StreamLoc123", deserialized.getId().getName(), "The stream location ID name does not match"),
                () -> assertEquals("SPK", deserialized.getId().getOfficeId(), "The stream location ID officeId does not match"),
                () -> assertEquals("ImOnThisStream", deserialized.getStreamNode().getStreamId().getName(), "The flows into stream ID name does not match"),
                () -> assertEquals("SPK", deserialized.getStreamNode().getStreamId().getOfficeId(), "The flows into stream ID officeId does not match"),
                () -> assertEquals(123.45, deserialized.getPublishedStation(), "The published station does not match"),
                () -> assertEquals(12, deserialized.getNavigationStation(), "The navigation station does not match"),
                () -> assertEquals(1.5, deserialized.getLowestMeasurableStage(), "The lowest measurable stage does not match"),
                () -> assertEquals(10.5, deserialized.getTotalDrainageArea(), "The total drainage area does not match"),
                () -> assertEquals(0.01, deserialized.getUngagedDrainageArea(), "The ungaged drainage area does not match"),
                () -> assertEquals("mi2", deserialized.getAreaUnits(), "The area unit does not match"),
                () -> assertEquals("ft", deserialized.getStageUnits(), "The stage unit does not match")
        );
    }
}