package cwms.cda.api.enums;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class VersionTypeTest {

    @Test
    void test_version_type_for(){
        assertEquals(VersionType.MAX_AGGREGATE, VersionType.versionTypeFor("max_aggregate"));
        assertEquals(VersionType.MAX_AGGREGATE, VersionType.versionTypeFor("MAX_AGGREGATE"));
        assertEquals(VersionType.SINGLE_VERSION, VersionType.versionTypeFor("single_version"));
        assertEquals(VersionType.SINGLE_VERSION, VersionType.versionTypeFor("SINGLE_VERSION"));
        assertEquals(VersionType.UNVERSIONED, VersionType.versionTypeFor("UNVERSIONED"));
        assertEquals(VersionType.UNVERSIONED, VersionType.versionTypeFor("unversioned"));

        // We could make VersionType handle the default to MAX_AGGREGATE but I think that might be
        // better to do explicitly in the Controller code.
        assertNull(VersionType.versionTypeFor(null));
        assertThrows(IllegalArgumentException.class, () -> VersionType.versionTypeFor("invalid"));
    }

}