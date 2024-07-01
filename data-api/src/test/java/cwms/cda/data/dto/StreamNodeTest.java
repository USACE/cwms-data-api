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
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public final class StreamNodeTest {

    @Test
    void createStreamNode_allFieldsProvided_success() {
        CwmsId flowsIntoStreamId = new CwmsId.Builder()
                .withOfficeId("SPK")
                .withName("AnotherStream")
                .build();

        StreamNode.Builder builder = new StreamNode.Builder()
                .withStreamId(flowsIntoStreamId)
                .withBank(Bank.LEFT)
                .withStation(123.45)
                .withStationUnits("mi");

        StreamNode item = builder.build();

        assertAll(() -> assertEquals(flowsIntoStreamId.getName(), item.getStreamId().getName(), "The flows into stream id does not match the provided value"),
                () -> assertEquals(flowsIntoStreamId.getOfficeId(), item.getStreamId().getOfficeId(), "The office id does not match the provided value"),
                () -> assertEquals(Bank.LEFT, item.getBank(), "The bank does not match the provided value"),
                () -> assertEquals(123.45, item.getStation(), "The station does not match the provided value"),
                () -> assertEquals("mi", item.getStationUnits(), "The station unit does not match the provided value"));
    }

    @Test
    void createStreamNode_missingField_throwsFieldException() {
        assertThrows(FieldException.class, () -> {
            StreamNode.Builder builder = new StreamNode.Builder()
                    .withBank(Bank.LEFT)
                    .withStation(123.45)
                    .withStationUnits("mi");
            StreamNode item = builder.build();
            item.validate();
        }, "The validate method should have thrown a FieldException because the stream id field is missing");
    }

    @Test
    void createStreamNode_serialize_roundtrip() {
        CwmsId flowsIntoStreamId = new CwmsId.Builder()
                .withOfficeId("SPK")
                .withName("AnotherStream")
                .build();

        StreamNode streamNode = new StreamNode.Builder()
                .withStreamId(flowsIntoStreamId)
                .withBank(Bank.LEFT)
                .withStation(123.45)
                .withStationUnits("mi")
                .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, streamNode);
        StreamNode deserialized = Formats.parseContent(contentType, json, StreamNode.class);
        assertSame(streamNode, deserialized);
    }

    @Test
    void createStreamNode_deserialize() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/stream_node.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        StreamNode deserialized = Formats.parseContent(contentType, json, StreamNode.class);

        assertAll(
                () -> assertEquals("SPK", deserialized.getStreamId().getOfficeId(), "The office ID does not match"),
                () -> assertEquals("Stream123", deserialized.getStreamId().getName(), "The stream ID name does not match"),
                () -> assertEquals(Bank.LEFT, deserialized.getBank(), "The bank does not match"),
                () -> assertEquals(123.45, deserialized.getStation(), "The station does not match")
        );
    }

    public static void assertSame(StreamNode node1, StreamNode node2) {
        assertAll(
            () -> CwmsIdTest.assertSame(node1.getStreamId(), node2.getStreamId()),
            () -> assertEquals(node1.getBank(), node2.getBank()),
            () -> assertEquals(node1.getStation(), node2.getStation()),
            () -> assertEquals(node1.getStationUnits(), node2.getStationUnits())
        );
    }

}