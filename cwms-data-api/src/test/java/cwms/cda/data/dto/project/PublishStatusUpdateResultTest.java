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

package cwms.cda.data.dto.project;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;


class PublishStatusUpdateResultTest {

    @Test
    void testCreate() {
        PublishStatusUpdateResult result = new PublishStatusUpdateResult(Instant.now());

        ContentType contentType = Formats.parseHeader(Formats.JSON, PublishStatusUpdateResult.class);
        String json = Formats.format(contentType, result);
        assertNotNull(json);
        assertTrue(json.contains("value")); // result is like: {"value":1719516704895}

    }

    @Test
    void testRoundtrip() throws IOException {
        InputStream stream = ProjectTest.class.getClassLoader().getResourceAsStream(
                "cwms/cda/data/dto/project_publish.json");
        assertNotNull(stream);
        String input = IOUtils.toString(stream, StandardCharsets.UTF_8);

        ObjectMapper om = JsonV2.buildObjectMapper();
        PublishStatusUpdateResult result = om.readValue(input, PublishStatusUpdateResult.class);
        assertNotNull(result);

        String json = om.writeValueAsString(result);
        PublishStatusUpdateResult result2 = om.readValue(json, PublishStatusUpdateResult.class);
        assertNotNull(result2);

        Instant value = result.getValue();
        Instant value2 = result2.getValue();
        assertEquals(value, value2);

    }

}