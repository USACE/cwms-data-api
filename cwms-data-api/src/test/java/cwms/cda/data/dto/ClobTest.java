package cwms.cda.data.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.api.ClobController;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;

import javax.xml.bind.JAXBException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

        Clob clob2 = Formats.parseContent(Formats.parseHeader(Formats.XMLV2), output, Clob.class);

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

        Clob clob2 = Formats.parseContent(Formats.parseHeader(Formats.XMLV2), output, Clob.class);

        assertNotNull(clob2);

        assertEquals(clob.getId(), clob2.getId());
        assertEquals(clob.getOfficeId(), clob2.getOfficeId());
        assertEquals(clob.getDescription(), clob2.getDescription());
        assertEquals(clob.getValue(), clob2.getValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.XMLV2, Formats.JSONV2})
    void testRoundtripXML2Clobs(String format)  {
        Clob clob = new Clob("MYOFFICE", "MYID", "MYDESC", "MYVALUE");
        Clobs clobs = new Clobs.Builder("cursor", 1, 1)
                .addClob(clob)
                .build();
        String output = Formats.format(Formats.parseHeader(format), clobs);

        Clobs clobs2 = Formats.parseContent(Formats.parseHeader(format), output, Clobs.class);

        assertNotNull(clobs2);

        assertEquals(1, clobs2.getPageSize());
        assertEquals(1, clobs2.getTotal());
        assertEquals("Y3Vyc29yfHwxfHwx", clobs2.getPage());
        assertEquals("TVlPRkZJQ0UvTVlJRDtERVNDUklQVElPTj1NWURFU0N8fDF8fDE=", clobs2.getNextPage());
        assertEquals(clob.getId(), clobs2.getClobs().get(0).getId());
        assertEquals(clob.getOfficeId(), clobs2.getClobs().get(0).getOfficeId());
        assertEquals(clob.getDescription(), clobs2.getClobs().get(0).getDescription());
        assertEquals(clob.getValue(), clobs2.getClobs().get(0).getValue());
    }

}
