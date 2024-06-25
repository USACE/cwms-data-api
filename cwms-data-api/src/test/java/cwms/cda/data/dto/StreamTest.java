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
import cwms.cda.data.dto.stream.Bank;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.data.dto.stream.StreamNode;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

class StreamTest {

    @Test
    void createStream_allFieldsProvided_success() {
        CwmsId cwmsId = new CwmsId.Builder()
                .withName("Stream123")
                .withOfficeId("SPK")
                .build();

        StreamNode flowsIntoStream = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder()
                        .withName("AnotherStream")
                        .withOfficeId("SPK")
                        .build())
                .withStation(123.45)
                .withBank(Bank.LEFT)
                .withStationUnit("mi")
                .build();

        StreamNode divertsFromStream = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder()
                        .withName("UpstreamStream")
                        .withOfficeId("SPK")
                        .build())
                .withStation(678.90)
                .withStationUnit("mi")
                .withBank(Bank.RIGHT)
                .build();

        Stream item = new Stream.Builder()
                .withStartsDownstream(true)
                .withFlowsIntoStreamNode(flowsIntoStream)
                .withDivertsFromStreamNode(divertsFromStream)
                .withLength(10.5)
                .withSlope(0.01)
                .withLengthUnit("mi")
                .withSlopeUnit("%")
                .withComment("This is a comment for the stream.")
                .withId(cwmsId)
                .build();

        assertAll(() -> assertEquals(true, item.getStartsDownstream(), "The starts downstream does not match the provided value"),
                () -> assertEquals(Bank.LEFT, item.getFlowsIntoStreamNode().getBank(), "Flows into stream bank"),
                () -> assertEquals(123.45, item.getFlowsIntoStreamNode().getStation(), 0.001, "Flows into stream station"),
                () -> assertEquals("mi", item.getFlowsIntoStreamNode().getStationUnit(), "Flows into stream station unit"),
                () -> assertEquals("AnotherStream", item.getFlowsIntoStreamNode().getStreamId().getName(), "Flows into stream name"),
                () -> assertEquals("SPK", item.getFlowsIntoStreamNode().getStreamId().getOfficeId(), "Flows into stream office id"),
                () -> assertEquals(Bank.RIGHT, item.getDivertsFromStreamNode().getBank(), "Diverts from stream bank"),
                () -> assertEquals(678.90, item.getDivertsFromStreamNode().getStation(), 0.001, "Diverts from stream station"),
                () -> assertEquals("mi", item.getDivertsFromStreamNode().getStationUnit(), "Diverts from stream station unit"),
                () -> assertEquals("UpstreamStream", item.getDivertsFromStreamNode().getStreamId().getName(), "Diverts from stream name"),
                () -> assertEquals("SPK", item.getDivertsFromStreamNode().getStreamId().getOfficeId(), "Diverts from stream office id"),
                () -> assertEquals(10.5, item.getLength(), 0.001, "Length"),
                () -> assertEquals(0.01, item.getSlope(), 0.001, "Slope"),
                () -> assertEquals("mi", item.getLengthUnit(), "Length unit"),
                () -> assertEquals("%", item.getSlopeUnit(), "Slope unit"),
                () -> assertEquals("This is a comment for the stream.", item.getComment(), "Comment"),
                () -> assertEquals("Stream123", item.getId().getName(), "Stream name"),
                () -> assertEquals("SPK", item.getId().getOfficeId(), "Stream office id"));
    }

    @Test
    void createStream_missingField_throwsFieldException() {
        assertThrows(FieldException.class, () -> {
            StreamNode flowsIntoStream = new StreamNode.Builder()
                    .withStreamId(new CwmsId.Builder()
                            .withName("AnotherStream")
                            .withOfficeId("SPK")
                            .build())
                    .withStation(123.45)
                    .withBank(Bank.LEFT)
                    .build();

            StreamNode divertsFromStream = new StreamNode.Builder()
                    .withStreamId(new CwmsId.Builder()
                            .withName("UpstreamStream")
                            .withOfficeId("SPK")
                            .build())
                    .withStation(678.90)
                    .withBank(Bank.RIGHT)
                    .build();

            Stream item = new Stream.Builder()
                    .withStartsDownstream(true)
                    .withFlowsIntoStreamNode(flowsIntoStream)
                    .withDivertsFromStreamNode(divertsFromStream)
                    .withLength(10.5)
                    .withSlope(0.01)
                    .withComment("This is a comment for the stream.")
                    .build();

            item.validate();
        }, "The validate method should have thrown a FieldException because the streamId field is missing");
    }

    @Test
    void createStream_serialize_roundtrip() {
        CwmsId cwmsId = new CwmsId.Builder()
                .withName("Stream123")
                .withOfficeId("SPK")
                .build();

        StreamNode flowsIntoStream = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder()
                        .withName("AnotherStream")
                        .withOfficeId("SPK")
                        .build())
                .withStation(123.45)
                .withBank(Bank.LEFT)
                .withStationUnit("mi")
                .build();

        StreamNode divertsFromStream = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder()
                        .withName("UpstreamStream")
                        .withOfficeId("SPK")
                        .build())
                .withStation(678.90)
                .withBank(Bank.RIGHT)
                .withStationUnit("mi")
                .build();

        Stream stream = new Stream.Builder()
                .withStartsDownstream(true)
                .withFlowsIntoStreamNode(flowsIntoStream)
                .withDivertsFromStreamNode(divertsFromStream)
                .withLength(10.5)
                .withSlope(0.01)
                .withComment("This is a comment for the stream.")
                .withId(cwmsId)
                .withSlopeUnit("%")
                .withLengthUnit("mi")
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, stream);
        Stream deserialized = Formats.parseContent(contentType, json, Stream.class);

        assertSame(stream, deserialized);
    }

    @Test
    void createStream_deserialize() throws Exception {
        CwmsId cwmsId = new CwmsId.Builder()
                .withName("Stream123")
                .withOfficeId("SPK")
                .build();

        StreamNode flowsIntoStream = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder()
                        .withName("AnotherStream")
                        .withOfficeId("SPK")
                        .build())
                .withStation(123.45)
                .withBank(Bank.LEFT)
                .withStationUnit("mi")
                .build();

        StreamNode divertsFromStream = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder()
                        .withName("UpstreamStream")
                        .withOfficeId("SPK")
                        .build())
                .withStation(678.90)
                .withBank(Bank.RIGHT)
                .withStationUnit("mi")
                .build();

        Stream expectedStream = new Stream.Builder()
                .withStartsDownstream(true)
                .withFlowsIntoStreamNode(flowsIntoStream)
                .withDivertsFromStreamNode(divertsFromStream)
                .withLength(10.5)
                .withSlope(0.01)
                .withComment("This is a comment for the stream.")
                .withId(cwmsId)
                .withSlopeUnit("%")
                .withLengthUnit("mi")
                .build();

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/stream.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        Stream deserialized = Formats.parseContent(contentType, json, Stream.class);

        assertSame(expectedStream, deserialized);
    }

    public static void assertSame(Stream stream1, Stream stream2) {
        assertAll(
            () -> assertEquals(stream1.getStartsDownstream(), stream2.getStartsDownstream()),
            () -> StreamNodeTest.assertSame(stream1.getFlowsIntoStreamNode(), stream2.getFlowsIntoStreamNode()),
            () -> StreamNodeTest.assertSame(stream1.getDivertsFromStreamNode(), stream2.getDivertsFromStreamNode()),
            () -> assertEquals(stream1.getLength(), stream2.getLength()),
            () -> assertEquals(stream1.getSlope(), stream2.getSlope()),
            () -> assertEquals(stream1.getLengthUnit(), stream2.getLengthUnit()),
            () -> assertEquals(stream1.getSlopeUnit(), stream2.getSlopeUnit()),
            () -> assertEquals(stream1.getComment(), stream2.getComment()),
            () -> CwmsIdTest.assertSame(stream1.getId(), stream2.getId())
        );
    }
}