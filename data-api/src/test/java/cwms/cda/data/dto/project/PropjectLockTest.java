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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

class PropjectLockTest {


    @Test
    void testSerialize(){

        ProjectLock lock = new ProjectLock.Builder("SPK", "ProjectId", "ApplicationId").build();
        assertNotNull(lock);

        // minimal
        String json = Formats.format(new ContentType(Formats.JSON), lock);
        assertNotNull(json);

        assertTrue(json.contains("SPK"));
        assertTrue(json.contains("ProjectId"));
        assertTrue(json.contains("ApplicationId"));

        // full
        lock = new ProjectLock.Builder("SPK", "ProjectId", "ApplicationId")
                .withSessionMachine("SessionMachine")
                .withSessionProgram("SessionProgram")
                .withSessionUser("SessionUser")
                .withOsUser("OsUser")
                .withAcquireTime(Instant.now())
                .build();
        assertNotNull(lock);
        json = Formats.format(new ContentType(Formats.JSON), lock);
        assertNotNull(json);

        assertTrue(json.contains("SPK"));
        assertTrue(json.contains("ProjectId"));
        assertTrue(json.contains("ApplicationId"));
        assertTrue(json.contains("SessionMachine"));
        assertTrue(json.contains("SessionProgram"));
        assertTrue(json.contains("SessionUser"));
        assertTrue(json.contains("OsUser"));

    }


    @Test
    void testSerializeList() {
        List<ProjectLock> locks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ProjectLock lock = new ProjectLock.Builder("SPK", "ProjectId" + i, "ApplicationId").build();
            locks.add(lock);
        }

        String json = Formats.format(new ContentType(Formats.JSON), locks, ProjectLock.class);
        assertNotNull(json);

    }

    @Test
    void testDeserializeList() throws IOException {
        InputStream stream = PropjectLockTest.class.getClassLoader().getResourceAsStream(
                "cwms/cda/data/dto/project_locks.json");
        assertNotNull(stream);
        String input = IOUtils.toString(stream, StandardCharsets.UTF_8);

        ObjectMapper om = JsonV2.buildObjectMapper();
        List<ProjectLock> lock = om.readValue(input, new TypeReference <List<ProjectLock>>(){});

        assertNotNull(lock);
        assertFalse(lock.isEmpty());
    }

    @Test
    void testDeserialize() throws IOException {
        InputStream stream = PropjectLockTest.class.getClassLoader().getResourceAsStream(
                "cwms/cda/data/dto/project_lock.json");
        assertNotNull(stream);
        String input = IOUtils.toString(stream, StandardCharsets.UTF_8);

        ObjectMapper om = JsonV2.buildObjectMapper();
        ProjectLock lock = om.readValue(input, ProjectLock.class);

        assertNotNull(lock);
    }

    @Test
    void testDeserializeFull() throws IOException {
        InputStream stream = PropjectLockTest.class.getClassLoader().getResourceAsStream(
                "cwms/cda/data/dto/project_lock_full.json");
        assertNotNull(stream);
        String input = IOUtils.toString(stream, StandardCharsets.UTF_8);

        ObjectMapper om = JsonV2.buildObjectMapper();
        ProjectLock lock = om.readValue(input, ProjectLock.class);

        assertNotNull(lock);
        assertEquals("SPK", lock.getOfficeId());
        assertEquals("ProjectId", lock.getProjectId());
        assertEquals("ApplicationId", lock.getApplicationId());
        assertEquals("SessionUser", lock.getSessionUser());
        assertEquals("OsUser", lock.getOsUser());
        assertEquals("SessionProgram", lock.getSessionProgram());
        assertEquals("SessionMachine", lock.getSessionMachine());
        assertEquals(1719430112429L, lock.getAcquireTime().toEpochMilli());

        // write
        String json = Formats.format(new ContentType(Formats.JSON), lock);
        ProjectLock lock2 = om.readValue(json, ProjectLock.class);

        assertEquals(lock.getOfficeId(), lock2.getOfficeId());
        assertEquals(lock.getProjectId(), lock2.getProjectId());
        assertEquals(lock.getApplicationId(), lock2.getApplicationId());
        assertEquals(lock.getSessionUser(), lock2.getSessionUser());
        assertEquals(lock.getOsUser(), lock2.getOsUser());
        assertEquals(lock.getSessionProgram(), lock2.getSessionProgram());
        assertEquals(lock.getSessionMachine(), lock2.getSessionMachine());
        assertEquals(lock.getAcquireTime(), lock2.getAcquireTime());

    }
}