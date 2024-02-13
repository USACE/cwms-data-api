package cwms.cda.data.dto;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.json.JsonV2;
import org.junit.jupiter.api.Test;

class BlobTest {
    @Test
    void testRoundtripJSON() throws JsonProcessingException
    {

        Blob blob = new Blob("MYOFFICE", "MYID", "MYDESC", "MYTYPE", "MYVALUE".getBytes());

        ObjectMapper om = JsonV2.buildObjectMapper();

        String serializedBlob = om.writeValueAsString(blob);

        assertNotNull(serializedBlob);

        Blob blob2 = om.readValue(serializedBlob, Blob.class);
        assertNotNull(blob2);

        assertEquals(blob.getId(), blob2.getId());
        assertEquals(blob.getOfficeId(), blob2.getOfficeId());
        assertEquals(blob.getDescription(), blob2.getDescription());
        assertEquals(blob.getMediaTypeId(), blob2.getMediaTypeId());
        assertArrayEquals(blob.getValue(), blob2.getValue());
    }

}
