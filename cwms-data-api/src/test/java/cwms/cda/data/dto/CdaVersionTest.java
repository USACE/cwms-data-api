/*
 *
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import org.apache.commons.io.IOUtils;

final class CdaVersionTest {

    @Test
    void testSerializeRoundTripVersion() {
        CdaVersion version = new CdaVersion.Builder()
            .withVersion("1.0.0")
            .build();

        ContentType contentType = new ContentType(Formats.JSON);
        String versionString = Formats.format(contentType, version);

        CdaVersion fromJson = Formats.parseContent(contentType, versionString, CdaVersion.class);
        assertEquals(version.getVersion(), fromJson.getVersion(),
            "The version after serialization and deserialization should match the original");
    }

    @Test
    void testDeserializeVersion() throws Exception {
        CdaVersion version = new CdaVersion.Builder()
            .withVersion("1.0.0-new_feature_branch")
            .build();

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/cda_version.json");
        assertNotNull(resource);

        ContentType contentType = new ContentType(Formats.JSON);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        CdaVersion fromJson = Formats.parseContent(contentType, json, CdaVersion.class);
        assertEquals(version.getVersion(), fromJson.getVersion(),
            "The version read from the JSON file should match the expected value");
    }
}
