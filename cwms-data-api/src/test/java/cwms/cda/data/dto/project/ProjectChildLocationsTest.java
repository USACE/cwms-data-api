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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

class ProjectChildLocationsTest {

    public static final String OFFICE = "SPK";

    @Test
    void test_ctor() {
        CwmsId proj = buildId(OFFICE, "TestProject");

        List<CwmsId> embanks = new ArrayList<>();
        embanks.add(buildId(OFFICE, "TestEmbankment1"));
        embanks.add(buildId(OFFICE, "TestEmbankment2"));

        List<CwmsId> locks = new ArrayList<>();
        locks.add(buildId(OFFICE, "TestLock1"));
        locks.add(buildId(OFFICE, "TestLock2"));
        locks.add(buildId(OFFICE, "TestLock3"));

        List<CwmsId> gates = new ArrayList<>();
        gates.add(buildId(OFFICE, "TestGate1"));

        List<CwmsId> turbines = new ArrayList<>();
        turbines.add(buildId(OFFICE, "TestTurbine1"));

        List<CwmsId> outlets = new ArrayList<>();
        outlets.add(buildId(OFFICE, "TestOutlet1"));

        ProjectChildLocations projectChildLocations = new ProjectChildLocations.Builder()
                .withProjectId(proj)
                .withEmbankmentIds(embanks)
                .withLockIds(locks)
                .withGateIds(gates)
                .withTurbineIds(turbines)
                .withOutletIds(outlets)
                .build();
        assertNotNull(projectChildLocations);

        assertEquals(proj, projectChildLocations.getProjectId());
        assertEquals(embanks, projectChildLocations.getEmbankmentIds());
        assertEquals(locks, projectChildLocations.getLockIds());

        String json = Formats.format(new ContentType(Formats.JSON), projectChildLocations);

        assertNotNull(json);

    }

    private ProjectChildLocations buildTestProjectChildLocations(String office, String projectName) {
        CwmsId proj = buildId(office, projectName);

        List<CwmsId> embanks = new ArrayList<>();
        embanks.add(buildId(office, "TestEmbankment1"));
        embanks.add(buildId(office, "TestEmbankment2"));

        List<CwmsId> locks = new ArrayList<>();
        locks.add(buildId(office, "TestLock1"));
        locks.add(buildId(office, "TestLock2"));
        locks.add(buildId(office, "TestLock3"));

        List<CwmsId> gates = new ArrayList<>();
        gates.add(buildId(office, "TestGate1"));

        List<CwmsId> turbines = new ArrayList<>();
        turbines.add(buildId(office, "TestTurbine1"));

        List<CwmsId> outlets = new ArrayList<>();
        outlets.add(buildId(office, "TestOutlet1"));

        return new ProjectChildLocations.Builder()
                .withProjectId(proj)
                .withEmbankmentIds(embanks)
                .withLockIds(locks)
                .withGateIds(gates)
                .withTurbineIds(turbines)
                .withOutletIds(outlets)
                .build();
    }

    @Test
    void test_list_serialization() {
        List<ProjectChildLocations> list = new ArrayList<>();

        list.add(buildTestProjectChildLocations(OFFICE, "TestProject1"));
        list.add(buildTestProjectChildLocations(OFFICE, "TestProject2"));

        String json = Formats.format(new ContentType(Formats.JSON), list, ProjectChildLocations.class);
        assertNotNull(json);
        assertFalse(json.isEmpty());
        assertTrue(json.contains("TestProject1"));
        assertTrue(json.contains("TestProject2"));
    }

    @Test
    void test_list_deserialization() throws IOException {
        InputStream stream = ProjectTest.class.getClassLoader().getResourceAsStream(
                "cwms/cda/data/dto/project_children_list.json");
        assertNotNull(stream);
        String input = IOUtils.toString(stream, StandardCharsets.UTF_8);

        ObjectMapper om = JsonV2.buildObjectMapper();
        List<ProjectChildLocations> list = om.readValue(input, new TypeReference<List<ProjectChildLocations>>() {
        });

        assertNotNull(list);
        assertFalse(list.isEmpty());
    }

    @Test
    void testDeserialize() throws IOException {
        InputStream stream = ProjectTest.class.getClassLoader().getResourceAsStream(
                "cwms/cda/data/dto/project_children.json");
        assertNotNull(stream);
        String input = IOUtils.toString(stream, StandardCharsets.UTF_8);

        ObjectMapper om = JsonV2.buildObjectMapper();
        ProjectChildLocations projectChildLocations = om.readValue(input, ProjectChildLocations.class);
        assertNotNull(projectChildLocations);

        CwmsId project = projectChildLocations.getProjectId();
        assertNotNull(project);
        assertEquals("SPK", project.getOfficeId());
        assertEquals("TestProject", project.getName());

        List<CwmsId> embankments = projectChildLocations.getEmbankmentIds();
        assertNotNull(embankments);
        assertEquals(2, embankments.size());
        assertEquals("SPK", embankments.get(0).getOfficeId());
        assertEquals("TestEmbankment1", embankments.get(0).getName());
        assertEquals("SPK", embankments.get(1).getOfficeId());
        assertEquals("TestEmbankment2", embankments.get(1).getName());

        List<CwmsId> locks = projectChildLocations.getLockIds();
        assertNotNull(locks);
        assertEquals(3, locks.size());
        assertEquals("SPK", locks.get(0).getOfficeId());
        assertEquals("TestLock1", locks.get(0).getName());
        assertEquals("SPK", locks.get(1).getOfficeId());
        assertEquals("TestLock2", locks.get(1).getName());
        assertEquals("SPK", locks.get(2).getOfficeId());
        assertEquals("TestLock3", locks.get(2).getName());

        List<CwmsId> outlets = projectChildLocations.getOutletIds();
        assertNotNull(outlets);
        assertEquals(1, outlets.size());
        assertEquals("SPK", outlets.get(0).getOfficeId());
        assertEquals("TestOutlet1", outlets.get(0).getName());

        List<CwmsId> turbines = projectChildLocations.getTurbineIds();
        assertNotNull(turbines);
        assertEquals(1, turbines.size());
        assertEquals("SPK", turbines.get(0).getOfficeId());
        assertEquals("TestTurbine1", turbines.get(0).getName());

        List<CwmsId> gates = projectChildLocations.getGateIds();
        assertNotNull(gates);
        assertEquals(1, gates.size());
        assertEquals("SPK", gates.get(0).getOfficeId());
        assertEquals("TestGate1", gates.get(0).getName());
    }

    @Test
    void testRoundtrip() throws IOException {
        InputStream stream = ProjectTest.class.getClassLoader().getResourceAsStream(
                "cwms/cda/data/dto/project_children.json");
        assertNotNull(stream);
        String input = IOUtils.toString(stream, StandardCharsets.UTF_8);

        ObjectMapper om = JsonV2.buildObjectMapper();
        ProjectChildLocations projectChildLocations1 = om.readValue(input, ProjectChildLocations.class);
        assertNotNull(projectChildLocations1);

        String json = om.writeValueAsString(projectChildLocations1);
        ProjectChildLocations projectChildLocations2 = om.readValue(json, ProjectChildLocations.class);
        assertNotNull(projectChildLocations2);

        assertProjectChildLocationsEqual(projectChildLocations1, projectChildLocations2);
    }

    private static void assertProjectChildLocationsEqual(ProjectChildLocations projectChildLocations1,
                                                         ProjectChildLocations projectChildLocations2) {
        assertAll("ProjectChildLocations",
                () -> assertCwmsIdEqual(projectChildLocations1.getProjectId(), projectChildLocations2.getProjectId()),
                () -> assertListEqual(projectChildLocations1.getEmbankmentIds(), projectChildLocations2.getEmbankmentIds()),
                () -> assertListEqual(projectChildLocations1.getLockIds(), projectChildLocations2.getLockIds()),
                () -> assertListEqual(projectChildLocations1.getGateIds(), projectChildLocations2.getGateIds()),
                () -> assertListEqual(projectChildLocations1.getTurbineIds(), projectChildLocations2.getTurbineIds()),
                () -> assertListEqual(projectChildLocations1.getOutletIds(), projectChildLocations2.getOutletIds())
        );
    }

    private static void assertCwmsIdEqual(CwmsId project1, CwmsId project2) {
        assertAll("CwmsId",
                () -> assertEquals(project1.getOfficeId(), project2.getOfficeId()),
                () -> assertEquals(project1.getName(), project2.getName())
        );
    }

    private static void assertListEqual(List<CwmsId> locks1, List<CwmsId> locks2) {
        assertEquals(locks1.size(), locks2.size());
        for (int i = 0; i < locks1.size(); i++) {
            assertCwmsIdEqual(locks1.get(i), locks2.get(i));
        }
    }

    private static CwmsId buildId(String office, String name) {
        return new CwmsId.Builder()
                .withOfficeId(office)
                .withName(name)
                .build();
    }
}