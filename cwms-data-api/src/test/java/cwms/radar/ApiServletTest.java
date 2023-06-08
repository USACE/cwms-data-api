package cwms.radar;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

public class ApiServletTest {

    @Test
    public void test_office_from_context_hq(){
        String office = null;

        office = ApiServlet.officeFromContext("/cwms-data");
        assertTrue("HQ".equals(office),"failed to get HQ result cwms-data context");


        office = null;
        office = ApiServlet.officeFromContext("");
        assertTrue("HQ".equals(office),"failed to get HQ result on root context");

    }

    @ParameterizedTest
    @CsvSource( value = { "/spk-data,SPK", "/nwdm-data,NWDM", "/nww-data,NWW", "/swt-data,SWT"} )
    void test_office_from_context_district(String context, String office) {
        String returnedOffice = ApiServlet.officeFromContext(context);
        assertTrue(office.equals(returnedOffice), "failed to process an office context correctly");
    }
}
