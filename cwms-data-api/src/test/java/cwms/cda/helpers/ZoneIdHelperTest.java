package cwms.cda.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;

class ZoneIdHelperTest {

    @Test
    void test_buildDefaultAliases() {
        Map<String, String> aliases = ZoneIdHelper.buildDefaultAliases();
        
        // Verify the default aliases are correctly set
        assertEquals("Canada/Saskatchewan", aliases.get("Canada/East-Saskatchewan"));
        assertEquals("Asia/Taipei", aliases.get("ROC"));
        assertEquals("US/Pacific", aliases.get("US/Pacific-New"));
        assertEquals("UTC", aliases.get("Unknown or Not Applicable"));
        
        // Verify the size of the map
        assertEquals(4, aliases.size());
    }
    
    @Test
    void test_parseZoneIdWithAliases_validInput() {
        // Test with standard zone ID
        ZoneId zoneId = ZoneIdHelper.parseZoneIdWithAliases("UTC");
        assertEquals(ZoneId.of("UTC"), zoneId);
        
        // Test with aliased zone ID
        ZoneId aliasedZoneId = ZoneIdHelper.parseZoneIdWithAliases("US/Pacific-New");
        assertEquals(ZoneId.of("US/Pacific"), aliasedZoneId);
        
        // Test with another aliased zone ID
        ZoneId anotherAliasedZoneId = ZoneIdHelper.parseZoneIdWithAliases("Unknown or Not Applicable");
        assertEquals(ZoneId.of("UTC"), anotherAliasedZoneId);
    }
    
    @Test
    void test_parseZoneIdWithAliases_invalidInput() {
        // Test with invalid zone ID
        assertThrows(DateTimeException.class, () -> ZoneIdHelper.parseZoneIdWithAliases("InvalidZoneId"));
    }
    
    @Test
    void test_buildTimeZoneAliases() {
        Map<String, String> aliases = ZoneIdHelper.buildTimeZoneAliases();
        
        // Verify that the map is not null and not empty
        assertNotNull(aliases);
        assertFalse(aliases.isEmpty());
        
        // Verify that it contains at least the default aliases
        assertTrue(aliases.containsKey("Canada/East-Saskatchewan"));
        assertTrue(aliases.containsKey("ROC"));
        assertTrue(aliases.containsKey("US/Pacific-New"));
        assertTrue(aliases.containsKey("Unknown or Not Applicable"));
    }
    
    @Test
    void test_buildResourceAliases() {
        Map<String, String> aliases = ZoneIdHelper.buildResourceAliases();

        // we just verify that the method returns a map (which might be empty)
        assertNotNull(aliases);
    }
    
    @Test
    void test_buildResourceAliases_nonExistentResource() {
        Map<String, String> aliases = ZoneIdHelper.buildResourceAliases("non/existent/resource.properties");
        
        // Verify that an empty map is returned for a non-existent resource
        assertNotNull(aliases);
        assertTrue(aliases.isEmpty());
    }
    
    @Test
    void test_buildAliasMap_validProperties() {
        Properties props = new Properties();
        props.setProperty("alias.1.from", "TestFrom1");
        props.setProperty("alias.1.to", "TestTo1");
        props.setProperty("alias.2.from", "TestFrom2");
        props.setProperty("alias.2.to", "TestTo2");
        
        Map<String, String> aliases = ZoneIdHelper.buildAliasMap(props);
        
        // Verify the aliases are correctly built
        assertEquals("TestTo1", aliases.get("TestFrom1"));
        assertEquals("TestTo2", aliases.get("TestFrom2"));
        assertEquals(2, aliases.size());
    }
    
    @Test
    void test_buildAliasMap_emptyProperties() {
        Properties props = new Properties();
        
        Map<String, String> aliases = ZoneIdHelper.buildAliasMap(props);
        
        // Verify that an empty map is returned for empty properties
        assertNotNull(aliases);
        assertTrue(aliases.isEmpty());
    }
    
    @Test
    void test_buildAliasMap_malformedProperties() {
        Properties props = new Properties();
        // Missing 'to' property
        props.setProperty("alias.1.from", "TestFrom1");
        // Missing 'from' property
        props.setProperty("alias.2.to", "TestTo2");
        // Invalid key format
        props.setProperty("invalid.key", "InvalidValue");
        
        Map<String, String> aliases = ZoneIdHelper.buildAliasMap(props);
        
        // Verify that no aliases are created for malformed properties
        assertNotNull(aliases);
        assertTrue(aliases.isEmpty());
    }
    
    @Test
    void test_buildResourceAliases_customResource() throws IOException {
        // Create a test properties content
        String propertiesContent = 
                "alias.1.from=CustomFrom1\n" +
                "alias.1.to=CustomTo1\n" +
                "alias.2.from=CustomFrom2\n" +
                "alias.2.to=CustomTo2\n";
        

        Properties props = new Properties();
        try (InputStream is = new ByteArrayInputStream(propertiesContent.getBytes(StandardCharsets.UTF_8))) {
            props.load(is);
        }
        
        Map<String, String> aliases = ZoneIdHelper.buildAliasMap(props);
        
        // Verify the aliases are correctly built
        assertEquals("CustomTo1", aliases.get("CustomFrom1"));
        assertEquals("CustomTo2", aliases.get("CustomFrom2"));
        assertEquals(2, aliases.size());
    }
}