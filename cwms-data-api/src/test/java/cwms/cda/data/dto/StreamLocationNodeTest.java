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
import cwms.cda.data.dto.stream.StreamLocationNode;
import cwms.cda.data.dto.stream.StreamNode;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

public final class StreamLocationNodeTest {

    @Test
    void createStreamLocationNode_allFieldsProvided_success() {
        CwmsId streamLocationId = new CwmsId.Builder()
                .withOfficeId("SPK")
                .withName("StreamLocation123")
                .build();

        StreamNode streamNode = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder().withOfficeId("SPK").withName("AnotherStream").build())
                .withBank(Bank.LEFT)
                .withStation(123.45)
                .withStationUnits("mi")
                .build();

        StreamLocationNode.Builder builder = new StreamLocationNode.Builder()
                .withId(streamLocationId)
                .withStreamNode(streamNode);

        StreamLocationNode item = builder.build();

        assertAll(() -> assertEquals(streamLocationId.getName(), item.getId().getName(), "The stream location id does not match the provided value"),
                () -> assertEquals(streamLocationId.getOfficeId(), item.getId().getOfficeId(), "The office id does not match the provided value"),
                () -> StreamNodeTest.assertSame(streamNode, item.getStreamNode()));
    }

    @Test
    void createStreamLocationNode_missingField_throwsFieldException() {
        assertThrows(FieldException.class, () -> {
            StreamLocationNode.Builder builder = new StreamLocationNode.Builder()
                    .withStreamNode(new StreamNode.Builder()
                            .withBank(Bank.LEFT)
                            .withStation(123.45)
                            .withStationUnits("mi")
                            .build());
            StreamLocationNode item = builder.build();
            item.validate();
        }, "The validate method should have thrown a FieldException because the stream location id field is missing");

        assertThrows(FieldException.class, () -> {
            StreamLocationNode.Builder builder = new StreamLocationNode.Builder()
                    .withId(new CwmsId.Builder()
                            .withOfficeId("SPK")
                            .withName("StreamLocation123")
                            .build());
            StreamLocationNode item = builder.build();
            item.validate();
        }, "The validate method should have thrown a FieldException because the stream node field is missing");
    }

    @Test
    void createStreamLocationNode_serialize_roundtrip() {
        CwmsId streamLocationId = new CwmsId.Builder()
                .withOfficeId("SPK")
                .withName("StreamLocation123")
                .build();

        StreamNode streamNode = new StreamNode.Builder()
                .withStreamId(new CwmsId.Builder().withOfficeId("SPK").withName("AnotherStream").build())
                .withBank(Bank.LEFT)
                .withStation(123.45)
                .withStationUnits("mi")
                .build();

        StreamLocationNode streamLocationNode = new StreamLocationNode.Builder()
                .withId(streamLocationId)
                .withStreamNode(streamNode)
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, streamLocationNode);
        StreamLocationNode deserialized = Formats.parseContent(contentType, json, StreamLocationNode.class);
        assertSame(streamLocationNode, deserialized);
    }

    @Test
    void createStreamLocationNode_deserialize() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/stream_location_node.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        StreamLocationNode deserialized = Formats.parseContent(contentType, json, StreamLocationNode.class);

        assertAll(
                () -> assertEquals("SPK", deserialized.getId().getOfficeId(), "The office ID does not match"),
                () -> assertEquals("StreamLoc123", deserialized.getId().getName(), "The stream location ID name does not match"),
                () -> StreamNodeTest.assertSame(deserialized.getStreamNode(), new StreamNode.Builder()
                        .withStreamId(new CwmsId.Builder().withOfficeId("SPK").withName("Stream123").build())
                        .withBank(Bank.LEFT)
                        .withStation(123.45)
                        .withStationUnits("mi")
                        .build())
        );
    }

    public static void assertSame(StreamLocationNode node1, StreamLocationNode node2) {
        assertAll(
            () -> CwmsIdTest.assertSame(node1.getId(), node2.getId()),
            () -> StreamNodeTest.assertSame(node1.getStreamNode(), node2.getStreamNode())
        );
    }
}
