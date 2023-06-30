package cwms.cda;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.data.dao.JooqDao;
import io.javalin.core.validation.JavalinValidation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class ApiServletTest {

    @Test
    public void test_office_from_context_hq() {
        String office = null;

        office = ApiServlet.officeFromContext("/cwms-data");
        assertEquals("HQ", office, "failed to get HQ result cwms-data context");


        office = null;
        office = ApiServlet.officeFromContext("");
        assertEquals("HQ", office, "failed to get HQ result on root context");

    }

    @ParameterizedTest
    @CsvSource(value = {"/spk-data,SPK", "/nwdm-data,NWDM", "/nww-data,NWW", "/swt-data,SWT"})
    void test_office_from_context_district(String context, String office) {
        String returnedOffice = ApiServlet.officeFromContext(context);
        assertEquals(office, returnedOffice, "failed to process an office context correctly");
    }

    @Test
    public void test_deletemethod_registration() {
        ApiServlet.registerConverters();

        assertTrue(JavalinValidation.INSTANCE.hasConverter(JooqDao.DeleteMethod.class));
        JooqDao.DeleteMethod deleteMethod = JavalinValidation.INSTANCE.convertValue(JooqDao.DeleteMethod.class, "delete_data");
        assertEquals(JooqDao.DeleteMethod.DELETE_DATA, deleteMethod);
    }

}
