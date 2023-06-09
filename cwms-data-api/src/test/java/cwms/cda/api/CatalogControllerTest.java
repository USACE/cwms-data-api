package cwms.cda.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.sql.Connection;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.MetricRegistry;

import cwms.cda.formatters.FormattingException;

import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockFileDatabase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import fixtures.TestHttpServletResponse;
import fixtures.TestServletInputStream;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import io.javalin.http.HttpCode;
import io.javalin.http.util.ContextUtil;
import io.javalin.plugin.json.JavalinJackson;
import io.javalin.plugin.json.JsonMapperKt;

public class CatalogControllerTest extends ControllerTest{

    @Disabled // get all the infrastructure in place first.
    @ParameterizedTest
    @ValueSource(strings = {"blurge,","appliation/json+fred"})
    public void bad_formats_return_501(String format ) throws Exception {
        final String testBody = "test";
        CatalogController controller = spy(new CatalogController(new MetricRegistry()));

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        HashMap<String,Object> attributes = new HashMap<>();
        attributes.put(ContextUtil.maxRequestSizeKey,Integer.MAX_VALUE);

        when(request.getInputStream()).thenReturn(new TestServletInputStream(testBody));
        when(request.getAttribute("database")).thenReturn(this.conn);
        when(request.getRequestURI()).thenReturn("/catalog/TIMESERIES");

        Context context = ContextUtil.init(request,response,"*",new HashMap<String,String>(), HandlerType.GET,attributes);
        context.attribute("database",this.conn);


        assertNotNull( context.attribute("database"), "could not get the connection back as an attribute");

        when(request.getHeader(Header.ACCEPT)).thenReturn("BAD FORMAT");

        assertThrows( FormattingException.class, () -> {
            controller.getAll(context);
        });
    }

    @Test
    public void catalog_returns_only_original_ids_by_default() throws Exception {
        CatalogController controller = new CatalogController(new MetricRegistry());
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = new TestHttpServletResponse();

        HashMap<String,Object> attributes = new HashMap<>();
        attributes.put(ContextUtil.maxRequestSizeKey,Integer.MAX_VALUE);
        attributes.put(JsonMapperKt.JSON_MAPPER_KEY,new JavalinJackson());
        attributes.put("PolicyFactory", this.sanitizer);

        when(request.getInputStream()).thenReturn(new TestServletInputStream(""));
        when(request.getAttribute("database")).thenReturn(this.conn);
        when(request.getRequestURI()).thenReturn("/catalog/TIMESERIES");
        when(request.getHeader(Header.ACCEPT)).thenReturn("application/json;version=2");

        Context context = ContextUtil.init(request,
                                           response,
                                           "*",
                                           new HashMap<String,String>(),
                                           HandlerType.GET,
                                           attributes);
        context.attribute("database",this.conn);

        controller.getOne(context, CatalogableEndpoint.TIMESERIES.getValue());

        assertEquals(HttpCode.OK.getStatus(), response.getStatus(), "200 OK was not returned");
        assertNotNull(response.getOutputStream(), "Output stream wasn't created");

    }
}
