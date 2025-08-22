package cwms.cda.data.dao.project;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ProjectKindTest {

    @Test
    void test_embankment(){
        Set<ProjectKind> matches = ProjectKind.getMatchingKinds("EMBANKMENT");
        assertNotNull(matches);
        assertEquals(1, matches.size());
        assertTrue(matches.contains(ProjectKind.EMBANKMENT));
    }
    @Test
    void test_turbine(){
        Set<ProjectKind> matches = ProjectKind.getMatchingKinds("TURBINE");
        assertNotNull(matches);
        assertEquals(1, matches.size());
        assertTrue(matches.contains(ProjectKind.TURBINE));
    }

    @Test
    void test_outlet(){
        Set<ProjectKind> matches = ProjectKind.getMatchingKinds("OUTLET");
        assertNotNull(matches);
        assertEquals(1, matches.size());
        assertTrue(matches.contains(ProjectKind.OUTLET));
    }

    @Test
    void test_lock(){
        Set<ProjectKind> matches = ProjectKind.getMatchingKinds("LOCK");
        assertNotNull(matches);
        assertEquals(1, matches.size());
        assertTrue(matches.contains(ProjectKind.LOCK));
    }

    @Test
    void test_gate(){
        Set<ProjectKind> matches = ProjectKind.getMatchingKinds("GATE");
        assertNotNull(matches);
        assertEquals(1, matches.size());
        assertTrue(matches.contains(ProjectKind.GATE));
    }

    @Test
    void test_null(){
        Set<ProjectKind> matches = ProjectKind.getMatchingKinds(null);
        assertNotNull(matches);
        assertEquals(5, matches.size());
    }

    @Test
    void test_empty(){
        Set<ProjectKind> matches = ProjectKind.getMatchingKinds("");
        assertNotNull(matches);
        assertEquals(0, matches.size());
    }

    @Test
    void test_star(){
        Set<ProjectKind> matches = ProjectKind.getMatchingKinds(".*");
        assertNotNull(matches);
        assertEquals(5, matches.size());
    }

    @Test
    void test_pipe(){
        Set<ProjectKind> matches = ProjectKind.getMatchingKinds("^(EMBANKMENT|OUTLET)$");
        assertNotNull(matches);
        assertEquals(2, matches.size());
        assertTrue(matches.contains(ProjectKind.EMBANKMENT));
        assertTrue(matches.contains(ProjectKind.OUTLET));

        matches = ProjectKind.getMatchingKinds("EMBANKMENT|OUTLET");
        assertNotNull(matches);
        assertEquals(2, matches.size());
        assertTrue(matches.contains(ProjectKind.EMBANKMENT));
        assertTrue(matches.contains(ProjectKind.OUTLET));
    }

    @Test
    void test_star_t(){
        Set<ProjectKind> matches = ProjectKind.getMatchingKinds(".*T$");
        assertNotNull(matches);
        assertEquals(2, matches.size());
        assertTrue(matches.contains(ProjectKind.EMBANKMENT));
        assertTrue(matches.contains(ProjectKind.OUTLET));
    }


}