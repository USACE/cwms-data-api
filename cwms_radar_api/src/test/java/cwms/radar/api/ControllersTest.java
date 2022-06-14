package cwms.radar.api;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.MetricRegistry;
import cwms.radar.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import org.junit.jupiter.api.Test;

import static com.codahale.metrics.MetricRegistry.name;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ControllersTest {


    @Test
    void testCorrectQueryParams() {
        String nameToUse = "page-size";

        Context ctx = buildContext(nameToUse, 333);

        MetricRegistry metrics = new MetricRegistry();

        int pageSize = Controllers.queryParamAsClass(ctx, new String[]{"page-size", "pageSize", "pagesize"},
                Integer.class, 500, metrics, name(TimeSeriesController.class.getName(), "getAll"));
        assertEquals(333, pageSize);
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
        when(request.getRequestURL()).thenReturn(new StringBuffer("http://127.0.0.1:7001/timeseries/"));

        // build real context that uses the mock request/response
        Context ctx = new Context(request, response, map);

        return ctx;
    }

    @Test
    void testDeprecatedQueryParams() {
        String nameToUse = "pageSize";

        Context ctx = buildContext(nameToUse, 333);

        MetricRegistry metrics = new MetricRegistry();

        int pageSize = Controllers.queryParamAsClass(ctx, new String[]{"page-size", "pageSize", "pagesize"},
                Integer.class, 500, metrics, name(TimeSeriesController.class.getName(), "getAll"));
        assertEquals(333, pageSize);
    }

    @Test
    void testDeprecated3QueryParams() {
        String nameToUse = "pagesize";

        Context ctx = buildContext(nameToUse, 333);

        MetricRegistry metrics = new MetricRegistry();

        int pageSize = Controllers.queryParamAsClass(ctx, new String[]{"page-size", "pageSize", "pagesize"},
                Integer.class, 500, metrics, name(TimeSeriesController.class.getName(), "getAll"));
        assertEquals(333, pageSize);
    }

    @Test
    void testDefaultQueryParams() {
        String nameToUse = "fake";

        Context ctx = buildContext(nameToUse, 333);

        MetricRegistry metrics = new MetricRegistry();

        int pageSize = Controllers.queryParamAsClass(ctx, new String[]{"page-size", "pageSize", "pagesize"},
                Integer.class, 500, metrics, name(TimeSeriesController.class.getName(), "getAll"));
        assertEquals(500, pageSize);
    }
}