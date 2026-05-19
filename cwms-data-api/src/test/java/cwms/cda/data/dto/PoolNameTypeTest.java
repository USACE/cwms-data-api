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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

final class PoolNameTypeTest {

    @Test
    void testSerialization() throws Exception {
        InputStream is = PoolNameTypeTest.class.getResourceAsStream("/cwms/cda/data/dto/pool_name_type.json");
        assertNotNull(is);
        String pool = IOUtils.toString(is, StandardCharsets.UTF_8);

        String poolName = "Sacramento_River_Pool1";
        String officeId = "SPK";

        PoolNameType poolNameType = Formats.parseContent(new ContentType(Formats.JSON), pool, PoolNameType.class);

        assertEquals(poolName, poolNameType.getPoolName());
        assertEquals(officeId, poolNameType.getOfficeId());
    }

    @Test
    void testDeserialization() {
        InputStream is = PoolNameTypeTest.class.getResourceAsStream("/cwms/cda/data/dto/pool_name_type.json");
        assertNotNull(is);
        PoolNameType poolNameTypeIn = Formats.parseContent(new ContentType(Formats.JSON), is, PoolNameType.class);
        String pool = Formats.format(new ContentType(Formats.JSON), poolNameTypeIn);

        String poolName = "Sacramento_River_Pool1";
        String officeId = "SPK";

        PoolNameType poolNameType = new PoolNameType(poolName, officeId);

        assertEquals(poolName, poolNameType.getPoolName());
        assertEquals(officeId, poolNameType.getOfficeId());

        String poolNameTypeJson = Formats.format(new ContentType(Formats.JSON), poolNameType);

        assertEquals(pool, poolNameTypeJson);
    }
}
