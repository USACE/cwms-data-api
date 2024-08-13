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

package cwms.cda.data.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

final class JooqDaoTest {

    @Test
    void testFormatBool() {
        assertEquals("T", JooqDao.formatBool(true));
        assertEquals("F", JooqDao.formatBool(false));
        assertNull(JooqDao.formatBool(null));
    }

    @Test
    void testParseBool() {
        assertTrue(JooqDao.parseBool("T"));
        assertFalse(JooqDao.parseBool("F"));
        assertFalse(JooqDao.parseBool("ABC"));
        assertFalse(JooqDao.parseBool(null));
    }

    @Test
    void testToBigDecimal() {
        assertEquals(BigDecimal.valueOf(5.5), JooqDao.toBigDecimal(5.5));
        assertNull(JooqDao.toBigDecimal(null));
    }

    @Test
    void testBuildDouble() {
        assertEquals(5.5, JooqDao.buildDouble(BigDecimal.valueOf(5.5)), 0.0);
        assertEquals(0.0, JooqDao.buildDouble(null));
    }
}
