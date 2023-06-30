package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.core.validation.JavalinValidation;
import io.javalin.http.Context;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;

class ControllersTest {


    @Test
    void testCorrectQueryParams() {
        String nameToUse = "page-size";

        Context ctx = buildContext(nameToUse, 333);

        MetricRegistry metrics = new MetricRegistry();

        int pageSize = Controllers.queryParamAsClass(ctx, new String[]{"page-size", "pageSize",
                        "pagesize"},
                Integer.class, 500, metrics, name(TimeSeriesController.class.getName(), "getAll"));
        assertEquals(333, pageSize);
    }


    @Test
    void testBooleanQueryParamsLCTrue() {
        String nameToUse = "start_time_inclusive";

        Context ctx = buildContext(nameToUse, "true");

        Boolean flag = ctx.queryParamAsClass(nameToUse, Boolean.class).get();
        assertTrue(flag);

    }

    @Test
    void testBooleanQueryParamsUCTrue() {
        String nameToUse = "start_time_inclusive";

        Context ctx = buildContext(nameToUse, "True");

        Boolean flag = ctx.queryParamAsClass(nameToUse, Boolean.class).get();
        assertTrue(flag);

    }

    @Test
    void testBooleanQueryParamsUCT() {
        String nameToUse = "start_time_inclusive";

        Context ctx = buildContext(nameToUse, "T");

        Boolean flag = ctx.queryParamAsClass(nameToUse, Boolean.class).get();
        assertFalse(flag);  // This is false b/c Boolean.parseBoolean("T") returns false

    }

    @Test
    void testBooleanQueryParamsUCFalse() {
        String nameToUse = "start_time_inclusive";

        Context ctx = buildContext(nameToUse, "False");

        Boolean flag = ctx.queryParamAsClass(nameToUse, Boolean.class).get();
        assertFalse(flag);

    }

    @Test
    void testBooleanQueryParamsLCFalse() {
        String nameToUse = "start_time_inclusive";

        // Doesn't actually matter if its 'false' or 'False' - just that it's not true
        Context ctx = buildContext(nameToUse, "false");

        Boolean flag = ctx.queryParamAsClass(nameToUse, Boolean.class).get();
        assertFalse(flag);
    }

    @Test
    void testBooleanQueryParamsGarbageFalse() {
        String nameToUse = "start_time_inclusive";

        // 'garbage' is also not "true" so its false.
        Context ctx = buildContext(nameToUse, "garbage");

        Boolean flag = ctx.queryParamAsClass(nameToUse, Boolean.class).get();
        assertFalse(flag);
    }

    @Test
    void testBooleanQueryParamsNull() {
        String nameToUse = "start_time_inclusive";

        Context ctx = buildContext(nameToUse, null);

        // If its a Boolean flag and the user doesn't specify anything then javalin is going to throw an exception.
        try {
            Boolean flag = ctx.queryParamAsClass(nameToUse, Boolean.class).get();
            fail("Expected a ValidationException to be thrown");
        }catch (io.javalin.core.validation.ValidationException ve){
            // This is expected
            return;
        }

        // allowNullable() will skip the exception but return null;
        Boolean flag = ctx.queryParamAsClass(nameToUse, Boolean.class).allowNullable().get();
        assertNull(flag);

        // another option is to specify the default.
        flag = ctx.queryParamAsClass(nameToUse, Boolean.class).getOrDefault(false);
        assertFalse(flag);
    }


    private Context buildContext(String nameToUse, int expected) {
        // build mock request and response
        final HttpServletRequest request = mock(HttpServletRequest.class);
        final HttpServletResponse response = mock(HttpServletResponse.class);
        final Map<String, ?> map = new LinkedHashMap<>();

        when(request.getAttribute(nameToUse)).thenReturn(expected);

        when(request.getHeader(Header.ACCEPT)).thenReturn(Formats.JSONV2);

        Map<String, String> urlParams = new LinkedHashMap<>();
        urlParams.put(nameToUse, Integer.toString(expected));

        String paramStr = ControllerTest.buildParamStr(urlParams);

        when(request.getQueryString()).thenReturn(paramStr);
        when(request.getRequestURL()).thenReturn(new StringBuffer("http://127.0.0"
                + ".1:7001/timeseries/"));

        // build real context that uses the mock request/response
        Context ctx = new Context(request, response, map);

        return ctx;
    }

    private Context buildContext(String nameToUse, String expected) {
        // build mock request and response
        final HttpServletRequest request = mock(HttpServletRequest.class);
        final HttpServletResponse response = mock(HttpServletResponse.class);
        final Map<String, ?> map = new LinkedHashMap<>();

        when(request.getAttribute(nameToUse)).thenReturn(expected);

        when(request.getHeader(Header.ACCEPT)).thenReturn(Formats.JSONV2);

        Map<String, String> urlParams = new LinkedHashMap<>();
        if (expected != null) {
            urlParams.put(nameToUse, expected);
        }
        String paramStr = ControllerTest.buildParamStr(urlParams);

        when(request.getQueryString()).thenReturn(paramStr);
        when(request.getRequestURL()).thenReturn(new StringBuffer("http://127.0.0"
                + ".1:7001/timeseries/"));

        // build real context that uses the mock request/response
        Context ctx = new Context(request, response, map);

        return ctx;
    }

    @Test
    void testDeprecatedQueryParams() {
        String nameToUse = "pageSize";

        Context ctx = buildContext(nameToUse, 333);

        MetricRegistry metrics = new MetricRegistry();

        int pageSize = Controllers.queryParamAsClass(ctx, new String[]{"page-size", "pageSize",
                        "pagesize"},
                Integer.class, 500, metrics, name(TimeSeriesController.class.getName(), "getAll"));
        assertEquals(333, pageSize);
    }

    @Test
    void testDeprecated3QueryParams() {
        String nameToUse = "pagesize";

        Context ctx = buildContext(nameToUse, 333);

        MetricRegistry metrics = new MetricRegistry();

        int pageSize = Controllers.queryParamAsClass(ctx, new String[]{"page-size", "pageSize",
                        "pagesize"},
                Integer.class, 500, metrics, name(TimeSeriesController.class.getName(), "getAll"));
        assertEquals(333, pageSize);
    }

    @Test
    void testDefaultQueryParams() {
        String nameToUse = "fake";

        Context ctx = buildContext(nameToUse, 333);

        MetricRegistry metrics = new MetricRegistry();

        int pageSize = Controllers.queryParamAsClass(ctx, new String[]{"page-size", "pageSize",
                        "pagesize"},
                Integer.class, 500, metrics, name(TimeSeriesController.class.getName(), "getAll"));
        assertEquals(500, pageSize);
    }


    @Test
    void testDoubleQueryParam() {
        String nameToUse = "interval-forward";

        Context ctx = buildContext(nameToUse, "0");

        Number intervalForward = ctx.queryParamAsClass(nameToUse, Double.class).get();
        assertNotNull(intervalForward);

    }

    @Test
    void testDoubleQueryParamNull() {
        String nameToUse = "interval-forward";

        Context ctx = buildContext(nameToUse, null);

        Number intervalForward = ctx.queryParamAsClass(nameToUse, Double.class).getOrDefault(null);
        assertNull(intervalForward);

    }

    @Test
    void testDeleteMethod(){
        JooqDao.DeleteMethod deleteMethod = Controllers.getDeleteMethod(null);
        assertNull(deleteMethod);

        assertThrows(IllegalArgumentException.class, () -> Controllers.getDeleteMethod("garbage"));

        deleteMethod = Controllers.getDeleteMethod("delete_data");
        assertEquals(JooqDao.DeleteMethod.DELETE_DATA, deleteMethod);

        deleteMethod = Controllers.getDeleteMethod("DELETE_DATA");
        assertEquals(JooqDao.DeleteMethod.DELETE_DATA, deleteMethod);

        deleteMethod = Controllers.getDeleteMethod("delete_key");
        assertEquals(JooqDao.DeleteMethod.DELETE_KEY, deleteMethod);

        deleteMethod = Controllers.getDeleteMethod("delete_all");
        assertEquals(JooqDao.DeleteMethod.DELETE_ALL, deleteMethod);

        assertThrows(IllegalArgumentException.class, () -> Controllers.getDeleteMethod("delete-data"));
    }

    @Test
    void testDeleteMethodValidationRegistration() throws ClassNotFoundException {

        // Trigger static initialization of Controllers class
        Class<?> ignored = Class.forName("cwms.cda.api.Controllers");
        assertNotNull(ignored);

        assertTrue(JavalinValidation.INSTANCE.hasConverter(JooqDao.DeleteMethod.class));
        JooqDao.DeleteMethod deleteMethod = JavalinValidation.INSTANCE.convertValue(JooqDao.DeleteMethod.class, "delete_data");
        assertEquals(JooqDao.DeleteMethod.DELETE_DATA, deleteMethod);
    }
}