/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
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
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

final class StreamTest {

    @Test
    void createStream_allFieldsProvided_success() {
        LocationIdentifier locationIdentifier = new LocationIdentifier.Builder()
                .withLocationId("Stream123")
                .withOfficeId("Office123")
                .build();

        StreamJunctionIdentifier flowsIntoStream = new StreamJunctionIdentifier.Builder()
                .withStreamId(new LocationIdentifier.Builder()
                        .withLocationId("AnotherStream")
                        .withOfficeId("Office123")
                        .build())
                .withStation(123.45)
                .withBank("Left")
                .build();

        StreamJunctionIdentifier divertsFromStream = new StreamJunctionIdentifier.Builder()
                .withStreamId(new LocationIdentifier.Builder()
                        .withLocationId("UpstreamStream")
                        .withOfficeId("Office123")
                        .build())
                .withStation(678.90)
                .withBank("Right")
                .build();

        Stream item = new Stream.Builder()
                .withStartsDownstream(true)
                .withFlowsIntoStream(flowsIntoStream)
                .withDivertsFromStream(divertsFromStream)
                .withLength(10.5)
                .withSlope(0.01)
                .withComment("This is a comment for the stream.")
                .withStreamId(locationIdentifier)
                .build();

        assertAll(() -> assertEquals(true, item.getStartsDownstream(), "The starts downstream does not match the provided value"),
                () -> assertEquals(flowsIntoStream, item.getFlowsIntoStream(), "The flows into stream does not match the provided value"),
                () -> assertEquals(divertsFromStream, item.getDivertsFromStream(), "The diverts from stream does not match the provided value"),
                () -> assertEquals(10.5, item.getLength(), "The length does not match the provided value"),
                () -> assertEquals(0.01, item.getSlope(), "The slope does not match the provided value"),
                () -> assertEquals("This is a comment for the stream.", item.getComment(), "The comment does not match the provided value"),
                () -> assertEquals(locationIdentifier, item.getStreamId(), "The location identifier does not match the provided value"));
    }

    @Test
    void createStream_missingField_throwsFieldException() {
        assertAll(
                // When LocationIdentifier is missing
                () -> assertThrows(FieldException.class, () -> {
                    StreamJunctionIdentifier flowsIntoStream = new StreamJunctionIdentifier.Builder()
                            .withStreamId(new LocationIdentifier.Builder()
                                    .withLocationId("AnotherStream")
                                    .withOfficeId("Office123")
                                    .build())
                            .withStation(123.45)
                            .withBank("Left")
                            .build();

                    StreamJunctionIdentifier divertsFromStream = new StreamJunctionIdentifier.Builder()
                            .withStreamId(new LocationIdentifier.Builder()
                                    .withLocationId("UpstreamStream")
                                    .withOfficeId("Office123")
                                    .build())
                            .withStation(678.90)
                            .withBank("Right")
                            .build();

                    Stream item = new Stream.Builder()
                            .withStartsDownstream(true)
                            .withFlowsIntoStream(flowsIntoStream)
                            .withDivertsFromStream(divertsFromStream)
                            .withLength(10.5)
                            .withSlope(0.01)
                            .withComment("This is a comment for the stream.")
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the location identifier field is missing"));
    }

    @Test
    void createStream_serialize_roundtrip() {
        LocationIdentifier locationIdentifier = new LocationIdentifier.Builder()
                .withLocationId("Stream123")
                .withOfficeId("Office123")
                .build();

        StreamJunctionIdentifier flowsIntoStream = new StreamJunctionIdentifier.Builder()
                .withStreamId(new LocationIdentifier.Builder()
                        .withLocationId("AnotherStream")
                        .withOfficeId("Office123")
                        .build())
                .withStation(123.45)
                .withBank("Left")
                .build();

        StreamJunctionIdentifier divertsFromStream = new StreamJunctionIdentifier.Builder()
                .withStreamId(new LocationIdentifier.Builder()
                        .withLocationId("UpstreamStream")
                        .withOfficeId("Office123")
                        .build())
                .withStation(678.90)
                .withBank("Right")
                .build();

        Stream stream = new Stream.Builder()
                .withStartsDownstream(true)
                .withFlowsIntoStream(flowsIntoStream)
                .withDivertsFromStream(divertsFromStream)
                .withLength(10.5)
                .withSlope(0.01)
                .withComment("This is a comment for the stream.")
                .withStreamId(locationIdentifier)
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, stream);
        Stream deserialized = Formats.parseContent(contentType, json, Stream.class);
        assertEquals(stream, deserialized, "Stream deserialized from JSON doesn't equal original");
    }

    @Test
    void createStream_deserialize() throws Exception {
        LocationIdentifier locationIdentifier = new LocationIdentifier.Builder()
                .withLocationId("Stream123")
                .withOfficeId("Office123")
                .build();

        StreamJunctionIdentifier flowsIntoStream = new StreamJunctionIdentifier.Builder()
                .withStreamId(new LocationIdentifier.Builder()
                        .withLocationId("AnotherStream")
                        .withOfficeId("Office123")
                        .build())
                .withStation(123.45)
                .withBank("Left")
                .build();

        StreamJunctionIdentifier divertsFromStream = new StreamJunctionIdentifier.Builder()
                .withStreamId(new LocationIdentifier.Builder()
                        .withLocationId("UpstreamStream")
                        .withOfficeId("Office123")
                        .build())
                .withStation(678.90)
                .withBank("Right")
                .build();

        Stream stream = new Stream.Builder()
                .withStartsDownstream(true)
                .withFlowsIntoStream(flowsIntoStream)
                .withDivertsFromStream(divertsFromStream)
                .withLength(10.5)
                .withSlope(0.01)
                .withComment("This is a comment for the stream.")
                .withStreamId(locationIdentifier)
                .build();

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/stream.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        Stream deserialized = Formats.parseContent(contentType, json, Stream.class);
        assertEquals(stream, deserialized, "Stream deserialized from JSON doesn't equal original");
    }
}