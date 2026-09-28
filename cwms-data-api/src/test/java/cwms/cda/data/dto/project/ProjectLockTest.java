/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ProjectLockTest {
    private static final String OFFICE = "SPK";
    private static final String NAME = "ProjectId";
    private static final String APPLICATION_ID = "ApplicationId";

    static Stream<Fixture> fixtures() {
        return Stream.of(V1_FIXTURE, V2_FIXTURE);
    }

    @ParameterizedTest
    @MethodSource("fixtures")
    void testSerialize(Fixture fixture) {
        ProjectLockDTO lock = fixture.minimal(OFFICE, NAME, APPLICATION_ID);
        assertNotNull(lock);

        // minimal
        String json = Formats.format(new ContentType(Formats.JSON), lock);
        assertNotNull(json);

        assertTrue(json.contains(OFFICE));
        assertTrue(json.contains(NAME));
        assertTrue(json.contains(APPLICATION_ID));

        // full
        lock = fixture.full(OFFICE, NAME, APPLICATION_ID, Instant.now(), "SessionUser", "OsUser",
                "SessionProgram", "SessionMachine");
        assertNotNull(lock);
        json = Formats.format(new ContentType(Formats.JSON), lock);
        assertNotNull(json);

        assertTrue(json.contains(OFFICE));
        assertTrue(json.contains(NAME));
        assertTrue(json.contains(APPLICATION_ID));
        assertTrue(json.contains("SessionMachine"));
        assertTrue(json.contains("SessionProgram"));
        assertTrue(json.contains("SessionUser"));
        assertTrue(json.contains("OsUser"));
    }

    @ParameterizedTest
    @MethodSource("fixtures")
    void testSerializeList(Fixture fixture) {
        List<ProjectLockDTO> locks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            locks.add(fixture.minimal(OFFICE, NAME + i, APPLICATION_ID));
        }

        String json = Formats.format(new ContentType(Formats.JSON), locks, fixture.dtoClass());
        assertNotNull(json);
    }

    @ParameterizedTest
    @MethodSource("fixtures")
    void testDeserializeList(Fixture fixture) throws IOException {
        String input = readResource(fixture.listResource());

        ObjectMapper om = JsonV2.buildObjectMapper();
        CollectionType listType = om.getTypeFactory().constructCollectionType(List.class, fixture.dtoClass());
        List<ProjectLockDTO> locks = om.readValue(input, listType);

        assertNotNull(locks);
        assertFalse(locks.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("fixtures")
    void testDeserialize(Fixture fixture) throws IOException {
        String input = readResource(fixture.singleResource());

        ObjectMapper om = JsonV2.buildObjectMapper();
        ProjectLockDTO lock = om.readValue(input, fixture.dtoClass());

        assertNotNull(lock);
    }

    @ParameterizedTest
    @MethodSource("fixtures")
    void testDeserializeFull(Fixture fixture) throws IOException {
        String input = readResource(fixture.fullResource());

        ObjectMapper om = JsonV2.buildObjectMapper();
        ProjectLockDTO lock = om.readValue(input, fixture.dtoClass());

        assertNotNull(lock);
        assertEquals(OFFICE, fixture.office(lock));
        assertEquals(NAME, fixture.name(lock));
        assertEquals(APPLICATION_ID, lock.getApplicationId());
        assertEquals("SessionUser", lock.getSessionUser());
        assertEquals("OsUser", lock.getOsUser());
        assertEquals("SessionProgram", lock.getSessionProgram());
        assertEquals("SessionMachine", lock.getSessionMachine());
        assertEquals(1719430112429L, lock.getAcquireTime().toEpochMilli());

        // write
        String json = Formats.format(new ContentType(Formats.JSON), lock);
        ProjectLockDTO lock2 = om.readValue(json, fixture.dtoClass());

        assertEquals(fixture.office(lock), fixture.office(lock2));
        assertEquals(fixture.name(lock), fixture.name(lock2));
        assertEquals(lock.getApplicationId(), lock2.getApplicationId());
        assertEquals(lock.getSessionUser(), lock2.getSessionUser());
        assertEquals(lock.getOsUser(), lock2.getOsUser());
        assertEquals(lock.getSessionProgram(), lock2.getSessionProgram());
        assertEquals(lock.getSessionMachine(), lock2.getSessionMachine());
        assertEquals(lock.getAcquireTime(), lock2.getAcquireTime());
    }

    private static String readResource(String path) throws IOException {
        InputStream stream = ProjectLockTest.class.getClassLoader().getResourceAsStream(path);
        assertNotNull(stream);
        return IOUtils.toString(stream, StandardCharsets.UTF_8);
    }

    private interface Fixture {
        Class<? extends ProjectLockDTO> dtoClass();

        ProjectLockDTO minimal(String office, String name, String applicationId);

        ProjectLockDTO full(String office, String name, String applicationId, Instant acquireTime,
                            String sessionUser, String osUser, String sessionProgram, String sessionMachine);

        String name(ProjectLockDTO lock);

        String office(ProjectLockDTO lock);

        String singleResource();

        String fullResource();

        String listResource();
    }

    private static final Fixture V1_FIXTURE = new Fixture() {
        @Override
        public Class<ProjectLockV1> dtoClass() {
            return ProjectLockV1.class;
        }

        @Override
        public ProjectLockDTO minimal(String office, String name, String applicationId) {
            return new ProjectLockV1.Builder(office, name, applicationId).build();
        }

        @Override
        public ProjectLockDTO full(String office, String name, String applicationId, Instant acquireTime,
                                   String sessionUser, String osUser, String sessionProgram, String sessionMachine) {
            return new ProjectLockV1.Builder(office, name, applicationId)
                    .withAcquireTime(acquireTime)
                    .withSessionUser(sessionUser)
                    .withOsUser(osUser)
                    .withSessionProgram(sessionProgram)
                    .withSessionMachine(sessionMachine)
                    .build();
        }

        @Override
        public String name(ProjectLockDTO lock) {
            return ((ProjectLockV1) lock).getProjectId();
        }

        @Override
        public String office(ProjectLockDTO lock) {
            return ((ProjectLockV1) lock).getOfficeId();
        }

        @Override
        public String singleResource() {
            return "cwms/cda/data/dto/project_lock.json";
        }

        @Override
        public String fullResource() {
            return "cwms/cda/data/dto/project_lock_full.json";
        }

        @Override
        public String listResource() {
            return "cwms/cda/data/dto/project_locks.json";
        }

        @Override
        public String toString() {
            return "ProjectLockV1";
        }
    };

    private static final Fixture V2_FIXTURE = new Fixture() {
        @Override
        public Class<ProjectLockV2> dtoClass() {
            return ProjectLockV2.class;
        }

        @Override
        public ProjectLockDTO minimal(String office, String name, String applicationId) {
            return new ProjectLockV2.Builder(CwmsId.buildCwmsId(office, name), applicationId).build();
        }

        @Override
        public ProjectLockDTO full(String office, String name, String applicationId, Instant acquireTime,
                                   String sessionUser, String osUser, String sessionProgram, String sessionMachine) {
            return new ProjectLockV2.Builder(CwmsId.buildCwmsId(office, name), applicationId)
                    .withAcquireTime(acquireTime)
                    .withSessionUser(sessionUser)
                    .withOsUser(osUser)
                    .withSessionProgram(sessionProgram)
                    .withSessionMachine(sessionMachine)
                    .build();
        }

        @Override
        public String name(ProjectLockDTO lock) {
            return ((ProjectLockV2) lock).getId().getName();
        }

        @Override
        public String office(ProjectLockDTO lock) {
            return ((ProjectLockV2) lock).getId().getOfficeId();
        }

        @Override
        public String singleResource() {
            return "cwms/cda/data/dto/project_lock_v2.json";
        }

        @Override
        public String fullResource() {
            return "cwms/cda/data/dto/project_lock_full_v2.json";
        }

        @Override
        public String listResource() {
            return "cwms/cda/data/dto/project_locks_v2.json";
        }

        @Override
        public String toString() {
            return "ProjectLockV2";
        }
    };
}
