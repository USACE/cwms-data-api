package cwms.cda;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.javalin.http.Handler;
import io.javalin.http.HandlerEntry;
import io.javalin.http.HandlerType;
import io.javalin.http.PathMatcher;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class ApiServletTest {

    @Test
    public void test_office_from_context_hq(){
        String office;

        office = ApiServlet.officeFromContext("/cwms-data");
        assertEquals("HQ", office, "failed to get HQ result cwms-data context");

        office = ApiServlet.officeFromContext("");
        assertEquals("HQ", office, "failed to get HQ result on root context");
    }

    @ParameterizedTest
    @CsvSource( value = { "/spk-data,SPK", "/nwdm-data,NWDM", "/nww-data,NWW", "/swt-data,SWT"} )
    void test_office_from_context_district(String context, String office) {
        String returnedOffice = ApiServlet.officeFromContext(context);
        assertEquals(office, returnedOffice, "failed to process an office context correctly");
    }

    @Test
    public void test_adding_two_matchers_matches_both() {

        PathMatcher matcher = new PathMatcher();
        Handler handler = (ctx) -> {};
        matcher.add(new HandlerEntry(HandlerType.AFTER, "/offices/{office}", true, handler, handler));
        matcher.add(new HandlerEntry(HandlerType.AFTER, "/offices", true, handler, handler));


        List<HandlerEntry> matches = matcher.findEntries(HandlerType.AFTER, "/offices/SPK");
        assertNotNull(matches);
        assertFalse(matches.isEmpty());

        matches = matcher.findEntries(HandlerType.AFTER, "/offices/SPK/");
        assertNotNull(matches);
        assertFalse(matches.isEmpty());

        matches = matcher.findEntries(HandlerType.AFTER, "/offices");
        assertNotNull(matches);
        assertFalse(matches.isEmpty());

    }


    @Test
    public void test_match_with_internal_resource() {
        PathMatcher matcher = new PathMatcher();
        Handler handler = (ctx) -> {};
        matcher.add(new HandlerEntry(HandlerType.AFTER, "/levels/{level-id}/timeseries", true, handler, handler));

        List<HandlerEntry> matches  = matcher.findEntries(HandlerType.AFTER, "/levels/doesthismatch/timeseries");
        assertNotNull(matches);
        assertFalse(matches.isEmpty());

    }

    @Test
    public void test_match_with_internal_star() {
        PathMatcher matcher = new PathMatcher();
        Handler handler = (ctx) -> {};
        matcher.add(new HandlerEntry(HandlerType.AFTER, "/levels/*/timeseries", true, handler, handler));

        List<HandlerEntry> matches  = matcher.findEntries(HandlerType.AFTER, "/levels/doesthismatch/timeseries");
        assertNotNull(matches);
        assertFalse(matches.isEmpty());

    }


}


