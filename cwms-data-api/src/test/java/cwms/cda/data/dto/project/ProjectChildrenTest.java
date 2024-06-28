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

class ProjectChildrenTest {

    public static final String OFFICE = "SPK";

    @Test
    void test_ctor(){
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

        ProjectChildren projectChildren = new ProjectChildren.Builder()
                .withProject(proj)
                .withEmbankments(embanks)
                .withLocks(locks)
                .withGates(gates)
                .withTurbines(turbines)
                .withOutlets(outlets)
                .build();
        assertNotNull(projectChildren);

        assertEquals(proj, projectChildren.getProject());
        assertEquals(embanks, projectChildren.getEmbankments());
        assertEquals(locks, projectChildren.getLocks());

        String json = Formats.format(new ContentType(Formats.JSON), projectChildren);


        assertNotNull(json);

    }

    private ProjectChildren buildTestProjectChildren(String office, String projectName) {
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

        return new ProjectChildren.Builder()
                .withProject(proj)
                .withEmbankments(embanks)
                .withLocks(locks)
                .withGates(gates)
                .withTurbines(turbines)
                .withOutlets(outlets)
                .build();
    }

    @Test
    void test_list_serialization(){
        List<ProjectChildren> list = new ArrayList<>();

        list.add(buildTestProjectChildren(OFFICE, "TestProject1"));
        list.add(buildTestProjectChildren(OFFICE, "TestProject2"));

        String json = Formats.format(new ContentType(Formats.JSON), list, ProjectChildren.class);
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
        List<ProjectChildren> list = om.readValue(input, new TypeReference<List<ProjectChildren>>(){});

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
        ProjectChildren projectChildren = om.readValue(input, ProjectChildren.class);
        assertNotNull(projectChildren);

        CwmsId project = projectChildren.getProject();
        assertNotNull(project);
        assertEquals("SPK", project.getOfficeId());
        assertEquals("TestProject", project.getName());

        List<CwmsId> embankments = projectChildren.getEmbankments();
        assertNotNull(embankments);
        assertEquals(2, embankments.size());
        assertEquals("SPK", embankments.get(0).getOfficeId());
        assertEquals("TestEmbankment1", embankments.get(0).getName());
        assertEquals("SPK", embankments.get(1).getOfficeId());
        assertEquals("TestEmbankment2", embankments.get(1).getName());

        List<CwmsId> locks = projectChildren.getLocks();
        assertNotNull(locks);
        assertEquals(3, locks.size());
        assertEquals("SPK", locks.get(0).getOfficeId());
        assertEquals("TestLock1", locks.get(0).getName());
        assertEquals("SPK", locks.get(1).getOfficeId());
        assertEquals("TestLock2", locks.get(1).getName());
        assertEquals("SPK", locks.get(2).getOfficeId());
        assertEquals("TestLock3", locks.get(2).getName());

        List<CwmsId> outlets = projectChildren.getOutlets();
        assertNotNull(outlets);
        assertEquals(1, outlets.size());
        assertEquals("SPK", outlets.get(0).getOfficeId());
        assertEquals("TestOutlet1", outlets.get(0).getName());

        List<CwmsId> turbines = projectChildren.getTurbines();
        assertNotNull(turbines);
        assertEquals(1, turbines.size());
        assertEquals("SPK", turbines.get(0).getOfficeId());
        assertEquals("TestTurbine1", turbines.get(0).getName());

        List<CwmsId> gates = projectChildren.getGates();
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
        ProjectChildren projectChildren1 = om.readValue(input, ProjectChildren.class);
        assertNotNull(projectChildren1);

        String json = om.writeValueAsString(projectChildren1);
        ProjectChildren projectChildren2 = om.readValue(json, ProjectChildren.class);
        assertNotNull(projectChildren2);

        assertProjectChildrenEqual(projectChildren1, projectChildren2);

    }

    private static void assertProjectChildrenEqual(ProjectChildren projectChildren1, ProjectChildren projectChildren2) {
        assertAll("ProjectChildren",
                () -> assertCwmsIdEqual(projectChildren1.getProject(), projectChildren2.getProject()),
                () -> assertListEqual(projectChildren1.getEmbankments(), projectChildren2.getEmbankments()),
                () -> assertListEqual(projectChildren1.getLocks(), projectChildren2.getLocks()),
                () -> assertListEqual(projectChildren1.getGates(), projectChildren2.getGates()),
                () -> assertListEqual(projectChildren1.getTurbines(), projectChildren2.getTurbines()),
                () -> assertListEqual(projectChildren1.getOutlets(), projectChildren2.getOutlets())
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
        for(int i = 0; i < locks1.size(); i++){
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