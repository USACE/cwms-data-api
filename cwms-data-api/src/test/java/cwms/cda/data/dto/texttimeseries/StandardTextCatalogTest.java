package cwms.cda.data.dto.texttimeseries;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.json.JsonV2;
import java.util.Collection;
import org.junit.jupiter.api.Test;

class StandardTextCatalogTest {

    @Test
    void testSerialize() throws JsonProcessingException {
        StandardTextCatalog.Builder builder = new StandardTextCatalog.Builder();
        builder.withValue(build("CWMS", "E", "ESTIMATED"));
        builder.withValue(build("CWMS", "P", "INTERPOLATED"));

        StandardTextCatalog catalog = builder.build();
        assertNotNull(catalog);

        ObjectMapper mapper = JsonV2.buildObjectMapper();
        String json = mapper.writeValueAsString(catalog);
        assertNotNull(json);
//        System.out.println(json);

        assertTrue(json.contains("CWMS"));
        assertTrue(json.contains("E"));
        assertTrue(json.contains("P"));
        assertTrue(json.contains("ESTIMATED"));
        assertTrue(json.contains("INTERPOLATED"));
    }
 

    private static StandardTextId buildID(String E, String CWMS) {
        return new StandardTextId.Builder()
                .withId(E)
                .withOfficeId(CWMS).build();
    }

    private static StandardTextValue build(String officeId, String id, String text){
        return new StandardTextValue.Builder()
                .withId(buildID(id, officeId))
                .withStandardText(text).build();
    }

    @Test
    void testRoundtrip() throws JsonProcessingException {
        StandardTextCatalog.Builder builder = new StandardTextCatalog.Builder();
        builder.withValue(build("CWMS", "E", "ESTIMATED"));
        builder.withValue(build("CWMS", "P", "INTERPOLATED"));

        StandardTextCatalog catalog = builder.build();
        assertNotNull(catalog);

        ObjectMapper mapper = JsonV2.buildObjectMapper();
        String json = mapper.writeValueAsString(catalog);
        assertNotNull(json);

        StandardTextCatalog catalog2 = mapper.readValue(json, StandardTextCatalog.class);

        assertNotNull(catalog2);
        assertCatalogEquals(catalog, catalog2);

    }

    private static void assertCatalogEquals(StandardTextCatalog catalog, StandardTextCatalog catalog2) {
        assertEquals(catalog.getOfficeId(), catalog2.getOfficeId());

        assertEquals(catalog.getValues().size(), catalog2.getValues().size());
        for (StandardTextValue value : catalog.getValues()) {
            assertTrue(catalog2.getValues().contains(value));
        }

        for (StandardTextValue value : catalog2.getValues()) {
            assertTrue(catalog.getValues().contains(value));
        }
    }

    @Test
    void testOneOfEach() throws JsonProcessingException {
        // This test is a reminder of what this class is all about.  
        // The "Standard" in StdText is b/c there is a standardized set of common text values.
        // Its sort of like a standard marker.  
        // The available values can change (or maybe can just be added to) here is what I had in
        // my database:
        //        select * from "CWMS_20"."AV_STD_TEXT";
        //        +---------+-----------+-------------------------+
        //        |OFFICE_ID|STD_TEXT_ID|LONG_TEXT                |
        //        +---------+-----------+-------------------------+
        //        |CWMS     |A          |NO RECORD                |
        //        |CWMS     |B          |CHANNEL DRY              |
        //        |CWMS     |C          |POOL STAGE               |
        //        |CWMS     |D          |AFFECTED BY WIND         |
        //        |CWMS     |E          |ESTIMATED                |
        //        |CWMS     |F          |NOT AT STATED TIME       |
        //        |CWMS     |G          |GATES CLOSED             |
        //        |CWMS     |H          |PEAK STAGE               |
        //        |CWMS     |I          |ICE/SHORE ICE            |
        //        |CWMS     |J          |INTAKES OUT OF WATER     |
        //        |CWMS     |K          |FLOAT FROZEN/FLOATING ICE|
        //        |CWMS     |L          |GAGE FROZEN              |
        //        |CWMS     |M          |MALFUNCTION              |
        //        |CWMS     |N          |MEAN STAGE FOR THE DAY   |
        //        |CWMS     |O          |OBSERVERS READING        |
        //        |CWMS     |P          |INTERPOLATED             |
        //        |CWMS     |Q          |DISCHARGE MISSING        |
        //        |CWMS     |R          |HIGH WATER, NO ACCESS    |
        //        +---------+-----------+-------------------------+

        StandardTextId.Builder idBuilder =  new StandardTextId.Builder().withOfficeId("CWMS");
        StandardTextValue.Builder builder =  new StandardTextValue.Builder();

        // Lets make a catalog with one of each. 
        StandardTextCatalog catalog = new StandardTextCatalog.Builder()
                .withValue(build("CWMS", "A", "NO RECORD"))
                .withValue(build("CWMS", "B", "CHANNEL DRY"))
                .withValue(build("CWMS", "C", "POOL STAGE"))
                .withValue(build("CWMS", "D", "AFFECTED BY WIND"))
                .withValue(build("CWMS", "E", "ESTIMATED"))
                .withValue(build("CWMS", "F", "NOT AT STATED TIME"))
                .withValue(build("CWMS", "G", "GATES CLOSED"))
                .withValue(build("CWMS", "H", "PEAK STAGE"))
                .withValue(build("CWMS", "I", "ICE/SHORE ICE"))
                .withValue(build("CWMS", "J", "INTAKES OUT OF WATER"))
                .withValue(build("CWMS", "K", "FLOAT FROZEN/FLOATING ICE"))
                .withValue(build("CWMS", "L", "GAGE FROZEN"))
                .withValue(build("CWMS", "M", "MALFUNCTION"))
                .withValue(build("CWMS", "N", "MEAN STAGE FOR THE DAY"))
                .withValue(build("CWMS", "O", "OBSERVERS READING"))
                .withValue(build("CWMS", "P", "INTERPOLATED"))
                .withValue(build("CWMS", "Q", "DISCHARGE MISSING"))
                .withValue(build("CWMS", "R", "HIGH WATER, NO ACCESS"))
                .build();

        assertNotNull(catalog);
        Collection<StandardTextValue> values = catalog.getValues();
        assertNotNull(values);
        assertEquals(18, values.size());

        ObjectMapper mapper = JsonV2.buildObjectMapper();
        String json = mapper.writeValueAsString(catalog);
        assertNotNull(json);


    }

 
    
}