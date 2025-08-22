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
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

class LockRevokerRightsTest {

    @Test
    void testSerialize(){
        LockRevokerRights rights = new LockRevokerRights.Builder("SPK", "ProjectId", "ApplicationId", "UserId").build();
        assertNotNull(rights);

        String json = Formats.format(new ContentType(Formats.JSON), rights);
        assertNotNull(json);

        assertTrue(json.contains("SPK"));
        assertTrue(json.contains("ProjectId"));
        assertTrue(json.contains("ApplicationId"));
        assertTrue(json.contains("UserId"));

    }

    @Test
    void testDeserialize() throws IOException {
        InputStream stream = LockRevokerRightsTest.class.getClassLoader().getResourceAsStream(
                "cwms/cda/data/dto/lock_revoker_rights.json");
        assertNotNull(stream);
        String input = IOUtils.toString(stream, StandardCharsets.UTF_8);

        ObjectMapper om = JsonV2.buildObjectMapper();
        LockRevokerRights rights = om.readValue(input, LockRevokerRights.class);

        assertNotNull(rights);
        assertEquals("SPK", rights.getOfficeId());
        assertEquals("ProjectId", rights.getProjectId());
        assertEquals("ApplicationId", rights.getApplicationId());
        assertEquals("UserId", rights.getUserId());
    }

}