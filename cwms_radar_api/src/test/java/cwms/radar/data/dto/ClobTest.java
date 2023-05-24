package cwms.radar.data.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.api.ClobController;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.json.JsonV2;
import cwms.radar.formatters.xml.XMLv2;
import javax.xml.bind.JAXBException;
import org.junit.jupiter.api.Test;

class ClobTest {


    @Test
    void testRoundtripJSON() throws JsonProcessingException
    {
        Clob clob = new Clob("MYOFFICE", "MYID", "MYDESC", "MYVALUE");

        ObjectMapper om = JsonV2.buildObjectMapper();

        String serializedClob = om.writeValueAsString(clob);
        assertNotNull(serializedClob);

        Clob clob2 = om.readValue(serializedClob, Clob.class);
        assertNotNull(clob2);

        assertEquals(clob.getId(), clob2.getId());
        assertEquals(clob.getOfficeId(), clob2.getOfficeId());
        assertEquals(clob.getDescription(), clob2.getDescription());
        assertEquals(clob.getValue(), clob2.getValue());
    }


    @Test
    void testRoundtripXML() throws JAXBException {
        Clob clob = new Clob("MYOFFICE", "MYID", "MYDESC", "MYVALUE");

        XMLv2 xml = new XMLv2();
        String output = xml.format(clob);

        assertNotNull(output);

        Clob clob2 = ClobController.deserializeJAXB(output);

        assertNotNull(clob2);

        assertEquals(clob.getId(), clob2.getId());
        assertEquals(clob.getOfficeId(), clob2.getOfficeId());
        assertEquals(clob.getDescription(), clob2.getDescription());
        assertEquals(clob.getValue(), clob2.getValue());
    }

    @Test
    void testRoundtripXML2() throws JsonProcessingException {
        Clob clob = new Clob("MYOFFICE", "MYID", "MYDESC", "MYVALUE");

        XMLv2 xml = new XMLv2();
        String output = xml.format(clob);

        assertNotNull(output);

        // Output looks like:
        // <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        // <clob>
        //    <office>MYOFFICE</office>
        //    <id>MYID</id>
        //    <description>MYDESC</description>
        //    <value>MYVALUE</value>
        // </clob>

        ClobController controller = new ClobController(new MetricRegistry());
        Clob clob2 = controller.deserialize(output, Formats.XMLV2);

        assertNotNull(clob2);

        assertEquals(clob.getId(), clob2.getId());
        assertEquals(clob.getOfficeId(), clob2.getOfficeId());
        assertEquals(clob.getDescription(), clob2.getDescription());
        assertEquals(clob.getValue(), clob2.getValue());
    }



}