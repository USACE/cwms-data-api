package cwms.cda.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.flogger.FluentLogger;

import cwms.cda.data.dto.Clob;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import fixtures.TestServletInputStream;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import io.javalin.http.HttpResponseException;
import io.javalin.http.util.ContextUtil;
import io.javalin.plugin.json.JavalinJackson;
import io.javalin.plugin.json.JsonMapperKt;
import java.sql.SQLException;
import java.util.HashMap;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.Test;

public class ClobControllerTest extends ControllerTest{
    public static final FluentLogger logger = FluentLogger.forEnclosingClass();


    @Test
    public void bad_format_returns_501() throws Exception {

        final String testBody = "";
        ClobController controller = spy(new ClobController(new MetricRegistry()));
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        HashMap<String, Object> attributes = new HashMap<>();
        attributes.put(ContextUtil.maxRequestSizeKey, Integer.MAX_VALUE);
        attributes.put(JsonMapperKt.JSON_MAPPER_KEY, new JavalinJackson());

        when(request.getInputStream()).thenReturn(new TestServletInputStream(testBody));

        Context context = ContextUtil.init(request, response, "*", new HashMap<>(),
                HandlerType.GET, attributes);
        context.attribute("database", getTestConnection());

        when(request.getAttribute("database")).thenReturn(getTestConnection());

        assertNotNull(context.attribute("database"), "could not get the connection back as an attribute");

        when(request.getHeader(Header.ACCEPT)).thenReturn("BAD FORMAT");

        assertThrows(FormattingException.class, () -> controller.getAll(context));

    }

    /*
    @Test
    public void pagination_elements_returned_json(){
        given()
        .header("Accept","application/json;version=2").param("pageSize", 2)
        .get("/clobs")
        .then()
        .statusCode(HttpServletResponse.SC_OK)
        .body("$", hasKey("next-page"))
        .body("page-size", equalTo(2))
        .body("clobs.size()",is(2));

    }

    @Test
    public void pagination_elements_returned_xml(){
        given()
        .header("Accept","application/xml;version=2").param("pageSize", 2)
        .get("/clobs")
        .then()
        .statusCode(HttpServletResponse.SC_OK)
        .body(hasXPath("/clobs/nextPage"))
        .body("clobs.pageSize", equalTo("2"))
        .body("clobs.clobs.children().size()",is(2))
        ;
    }
    */

    // This is only supposed to test that when XML data is posted to create,
    // that data is forward to the method deserialize
    @Test
    void post_to_create_passed_to_deserializeJson() throws Exception
    {
        final String testBody = "could be anything";

        ClobController controller = spy(new ClobController(new MetricRegistry()));

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        HashMap<String,Object> attributes = new HashMap<>();
        attributes.put(ContextUtil.maxRequestSizeKey,Integer.MAX_VALUE);
        attributes.put(JsonMapperKt.JSON_MAPPER_KEY,new JavalinJackson());

        when(request.getInputStream()).thenReturn(new TestServletInputStream(testBody));

        Context context = ContextUtil.init(request,response,"*",new HashMap<>(), HandlerType.POST,attributes);

        when(request.getContentLength()).thenReturn(testBody.length());

        MockDataProvider provider = new MockDataProvider() {
            @Override
            public MockResult[] execute(MockExecuteContext mockExecuteContext) throws SQLException {
                fail();  // This test shouldn't call the database.  It just needs a DSLContext for the try/finally.
                return null;
            }
        };
        MockConnection connection = new MockConnection(provider);
        DSLContext dslContext = DSL.using(connection, SQLDialect.ORACLE);

        doReturn(dslContext).when(controller).getDslContext(context);

        when(request.getHeader(Header.ACCEPT)).thenReturn(Formats.JSONV2);
        when(request.getContentType()).thenReturn(Formats.JSONV2);

        logger.atFine().log( "Test post_to_create_passed_to_deserializeJson may trigger a exception - this is fine.");
        try {
            controller.create(context);
        } catch (HttpResponseException e){
            // It's throwing this exception bc deserialize can't parse our garbage data. That's fine.
            logger.atFine().log( "Test post_to_create_passed_to_deserializeJson caught an HttpResponseException - this is fine.");
        }
        // For this test, it's ok that the server throws a HttpResponseException
        // Only want to check that the controller accessed our mock dao in the expected way
        verify(controller, times(1)).deserialize(testBody, Formats.JSONV2);

    }

    @Test
    void testDeserialize() throws JsonProcessingException {
        String input = "{\"office-id\":\"MYOFFICE\",\"id\":\"MYID\",\"description\":\"MYDESC\","
                + "\"value\":\"MYVALUE\"}";

        ClobController controller = new ClobController(new MetricRegistry());

        Clob clob = controller.deserialize(input, Formats.JSONV2);
        assertNotNull(clob);
        assertEquals("MYOFFICE", clob.getOfficeId());
        assertEquals("MYID", clob.getId());
        assertEquals("MYDESC", clob.getDescription());
        assertEquals("MYVALUE", clob.getValue());
    }


}
