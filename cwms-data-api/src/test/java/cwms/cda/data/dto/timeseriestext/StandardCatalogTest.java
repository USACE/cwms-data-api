package cwms.cda.data.dto.timeseriestext;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.json.JsonV2;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class StandardCatalogTest {

    /*
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
     */


    @Test
    void testEntry() throws JsonProcessingException {
        StandardCatalog entry = new StandardCatalog("CWMS");
        entry.addValue("A", "NO RECORD");
        entry.addValue("B", "CHANNEL DRY");
        entry.addValue("C", "POOL STAGE");
        entry.addValue("D", "AFFECTED BY WIND");
        entry.addValue("E", "ESTIMATED");
        entry.addValue("F", "NOT AT STATED TIME");
        entry.addValue("G", "GATES CLOSED");
        entry.addValue("H", "PEAK STAGE");
        entry.addValue("I", "ICE/SHORE ICE");
        entry.addValue("J", "INTAKES OUT OF WATER");
        entry.addValue("K", "FLOAT FROZEN/FLOATING ICE");
        entry.addValue("L", "GAGE FROZEN");
        entry.addValue("M", "MALFUNCTION");
        entry.addValue("N", "MEAN STAGE FOR THE DAY");
        entry.addValue("O", "OBSERVERS READING");
        entry.addValue("P", "INTERPOLATED");
        entry.addValue("Q", "DISCHARGE MISSING");
        entry.addValue("R", "HIGH WATER, NO ACCESS");

        ObjectMapper mapper = JsonV2.buildObjectMapper();
        String json = mapper.writeValueAsString(entry);
        assertNotNull(json);
    }


    @Test
    void testEntries() throws JsonProcessingException {
        StandardCatalog catalog = new StandardCatalog("CWMS");
        catalog.addValue("A", "NO RECORD");
        catalog.addValue("B", "CHANNEL DRY");
        catalog.addValue("C", "POOL STAGE");
        catalog.addValue("D", "AFFECTED BY WIND");
        catalog.addValue("E", "ESTIMATED");
        catalog.addValue("F", "NOT AT STATED TIME");
        catalog.addValue("G", "GATES CLOSED");
        catalog.addValue("H", "PEAK STAGE");
        catalog.addValue("I", "ICE/SHORE ICE");
        catalog.addValue("J", "INTAKES OUT OF WATER");
        catalog.addValue("K", "FLOAT FROZEN/FLOATING ICE");
        catalog.addValue("L", "GAGE FROZEN");
        catalog.addValue("M", "MALFUNCTION");
        catalog.addValue("N", "MEAN STAGE FOR THE DAY");
        catalog.addValue("O", "OBSERVERS READING");
        catalog.addValue("P", "INTERPOLATED");
        catalog.addValue("Q", "DISCHARGE MISSING");
        catalog.addValue("R", "HIGH WATER, NO ACCESS");


        List<StandardCatalog> catalogs = new ArrayList<>();
        catalogs.add(catalog);

        StandardCatalog spkCat = new StandardCatalog("SPK");
        spkCat.addValue("Z", "END");
        spkCat.addValue("T", "TEST");
        catalogs.add(spkCat);


        ObjectMapper mapper = JsonV2.buildObjectMapper();
        String json = mapper.writeValueAsString(catalogs);
        assertNotNull(json);


    }

}