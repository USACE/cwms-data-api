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
import cwms.cda.data.dto.stream.StreamNode;
import cwms.cda.data.dto.stream.StreamReach;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public final class StreamReachTest {

    @Test
    void createStreamReach_allFieldsProvided_success() {
        CwmsId reachId = new CwmsId.Builder()
                .withName("Reach123")
                .withOfficeId("SPK")
                .build();
        CwmsId streamId = new CwmsId.Builder()
                .withName("Stream123")
                .withOfficeId("SPK")
                .build();

        StreamNode upstreamNode = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder()
                        .withName("Upstream123")
                        .withOfficeId("SPK")
                        .build())
                .withStation(10.0)
                .withBank(Bank.LEFT)
                .build();

        StreamNode downstreamNode = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder()
                        .withName("Downstream123")
                        .withOfficeId("SPK")
                        .build())
                .withStation(20.0)
                .withBank(Bank.RIGHT)
                .build();

        StreamReach item = new StreamReach.Builder()
                .withComment("This is a comment for the stream reach.")
                .withDownstreamNode(downstreamNode)
                .withUpstreamNode(upstreamNode)
                .withConfigurationId(new CwmsId.Builder()
                        .withName("Config123")
                        .withOfficeId("SPK")
                        .build())
                .withStreamId(streamId)
                .withId(reachId)
                .build();

        assertAll(() -> assertEquals("This is a comment for the stream reach.", item.getComment(), "The comment does not match the provided value"),
                () -> assertEquals(downstreamNode.getStreamId().getName(), item.getDownstreamNode().getStreamId().getName(), "The downstream node does not match the provided value"),
                () -> assertEquals(downstreamNode.getStation(), item.getDownstreamNode().getStation(), "The downstream node station does not match the provided value"),
                () -> assertEquals(downstreamNode.getBank(), item.getDownstreamNode().getBank(), "The downstream node bank does not match the provided value"),
                () -> assertEquals(downstreamNode.getStationUnits(), item.getDownstreamNode().getStationUnits(), "The downstream node station unit does not match the provided value"),
                () -> assertEquals(upstreamNode.getStreamId().getName(), item.getUpstreamNode().getStreamId().getName(), "The upstream node does not match the provided value"),
                () -> assertEquals(upstreamNode.getStation(), item.getUpstreamNode().getStation(), "The upstream node station does not match the provided value"),
                () -> assertEquals(upstreamNode.getBank(), item.getUpstreamNode().getBank(), "The upstream node does not match the provided value"),
                () -> assertEquals(upstreamNode.getStationUnits(), item.getUpstreamNode().getStationUnits(), "The upstream node station unit does not match the provided value"),
                () -> assertEquals("Config123", item.getConfigurationId().getName(), "The configuration ID name does not match the provided value"),
                () -> assertEquals("Stream123", item.getStreamId().getName(), "The stream ID name does not match the provided value"),
                () -> assertEquals("Reach123", item.getId().getName(), "The reach ID name does not match the provided value"));
    }

    @Test
    void createStreamReach_missingField_throwsFieldException() {
        CwmsId reachId = new CwmsId.Builder()
                .withName("Reach123")
                .withOfficeId("SPK")
                .build();
        CwmsId streamId = new CwmsId.Builder()
                .withName("Stream123")
                .withOfficeId("SPK")
                .build();

        StreamNode upstreamNode = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder()
                        .withName("Upstream123")
                        .withOfficeId("SPK")
                        .build())
                .withStation(10.0)
                .withBank(Bank.LEFT)
                .build();

        StreamNode downstreamNode = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder()
                        .withName("Downstream123")
                        .withOfficeId("SPK")
                        .build())
                .withStation(20.0)
                .withBank(Bank.RIGHT)
                .build();

        assertAll(
                // When ReachId is missing
                () -> assertThrows(FieldException.class, () -> {
                    StreamReach item = new StreamReach.Builder()
                            .withComment("This is a comment for the stream reach.")
                            .withDownstreamNode(downstreamNode)
                            .withUpstreamNode(upstreamNode)
                            .withConfigurationId(new CwmsId.Builder()
                                    .withName("Config123")
                                    .withOfficeId("SPK")
                                    .build())
                            .withStreamId(streamId)
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the reach ID field is missing"),

                // When StreamId is missing
                () -> assertThrows(FieldException.class, () -> {
                    StreamReach item = new StreamReach.Builder()
                            .withComment("This is a comment for the stream reach.")
                            .withDownstreamNode(downstreamNode)
                            .withUpstreamNode(upstreamNode)
                            .withConfigurationId(new CwmsId.Builder()
                                    .withName("Config123")
                                    .withOfficeId("SPK")
                                    .build())
                            .withId(reachId)
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the stream ID field is missing"),

                // When UpstreamNode is missing
                () -> assertThrows(FieldException.class, () -> {
                    StreamReach item = new StreamReach.Builder()
                            .withComment("This is a comment for the stream reach.")
                            .withDownstreamNode(downstreamNode)
                            .withConfigurationId(new CwmsId.Builder()
                                    .withName("Config123")
                                    .withOfficeId("SPK")
                                    .build())
                            .withStreamId(streamId)
                            .withId(reachId)
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the upstream node field is missing"),

                // When DownstreamNode is missing
                () -> assertThrows(FieldException.class, () -> {
                    StreamReach item = new StreamReach.Builder()
                            .withComment("This is a comment for the stream reach.")
                            .withUpstreamNode(upstreamNode)
                            .withConfigurationId(new CwmsId.Builder()
                                    .withName("Config123")
                                    .withOfficeId("SPK")
                                    .build())
                            .withStreamId(streamId)
                            .withId(reachId)
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the downstream node field is missing"),

                // When ConfigurationId is missing
                () -> assertThrows(FieldException.class, () -> {
                    StreamReach item = new StreamReach.Builder()
                            .withComment("This is a comment for the stream reach.")
                            .withDownstreamNode(downstreamNode)
                            .withUpstreamNode(upstreamNode)
                            .withStreamId(streamId)
                            .withId(reachId)
                            .build();
                    item.validate();
                }, "The validate method should have thrown a FieldException because the configuration ID field is missing"));
    }

    @Test
    void createStreamReach_serialize_roundtrip() {
        CwmsId reachId = new CwmsId.Builder()
                .withName("Reach123")
                .withOfficeId("SPK")
                .build();
        CwmsId streamId = new CwmsId.Builder()
                .withName("Stream123")
                .withOfficeId("SPK")
                .build();

        StreamNode upstreamNode = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder()
                        .withName("Upstream123")
                        .withOfficeId("SPK")
                        .build())
                .withStation(10.0)
                .withBank(Bank.LEFT)
                .build();

        StreamNode downstreamNode = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder()
                        .withName("Downstream123")
                        .withOfficeId("SPK")
                        .build())
                .withStation(20.0)
                .withBank(Bank.RIGHT)
                .build();

        StreamReach streamReach = new StreamReach.Builder()
                .withComment("This is a comment for the stream reach.")
                .withDownstreamNode(downstreamNode)
                .withUpstreamNode(upstreamNode)
                .withConfigurationId(new CwmsId.Builder()
                        .withName("Config123")
                        .withOfficeId("SPK")
                        .build())
                .withStreamId(streamId)
                .withId(reachId)
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, streamReach);
        StreamReach deserialized = Formats.parseContent(contentType, json, StreamReach.class);
        assertSame(streamReach, deserialized);
    }

    @Test
    void createStreamReach_deserialize() throws Exception {
        CwmsId reachId = new CwmsId.Builder()
                .withName("Reach123")
                .withOfficeId("SPK")
                .build();
        CwmsId streamId = new CwmsId.Builder()
                .withName("Stream123")
                .withOfficeId("SPK")
                .build();

        StreamNode upstreamNode = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder()
                        .withName("Upstream123")
                        .withOfficeId("SPK")
                        .build())
                .withStation(10.0)
                .withBank(Bank.LEFT)
                .withStationUnits("ft")
                .build();

        StreamNode downstreamNode = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder()
                        .withName("Downstream123")
                        .withOfficeId("SPK")
                        .build())
                .withStation(20.0)
                .withBank(Bank.RIGHT)
                .withStationUnits("ft")
                .build();

        StreamReach expected = new StreamReach.Builder()
                .withComment("This is a comment for the stream reach.")
                .withDownstreamNode(downstreamNode)
                .withUpstreamNode(upstreamNode)
                .withConfigurationId(new CwmsId.Builder()
                        .withName("Config123")
                        .withOfficeId("SPK")
                        .build())
                .withStreamId(streamId)
                .withId(reachId)
                .build();

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/stream_reach.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        StreamReach deserialized = Formats.parseContent(contentType, json, StreamReach.class);
        assertSame(expected, deserialized);
    }

    public static void assertSame(StreamReach reach1, StreamReach reach2)
    {
        assertAll(
            () -> assertEquals(reach1.getComment(), reach2.getComment()),
            () -> StreamNodeTest.assertSame(reach1.getDownstreamNode(), reach2.getDownstreamNode()),
            () -> StreamNodeTest.assertSame(reach1.getUpstreamNode(), reach2.getUpstreamNode()),
            () -> CwmsIdTest.assertSame(reach1.getConfigurationId(), reach2.getConfigurationId()),
            () -> CwmsIdTest.assertSame(reach1.getStreamId(), reach2.getStreamId()),
            () -> CwmsIdTest.assertSame(reach1.getId(), reach2.getId())
        );
    }
}