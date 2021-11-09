package mil.army.usace.hec;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


import java.io.InputStream;
import java.sql.Connection;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.MetricRegistry;

import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockFileDatabase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import cwms.radar.api.ClobController;
import cwms.radar.formatters.FormattingException;
import fixtures.TestServletInputStream;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import io.javalin.http.util.ContextUtil;
import io.javalin.plugin.json.JavalinJackson;
import io.javalin.plugin.json.JsonMapperKt;

import static io.restassured.RestAssured.*;

import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class ClobControllerTest {

    private Connection conn = null;


    @BeforeEach
    public void baseLineDbMocks() throws Exception{
        InputStream stream = RatingsControllerTest.class.getResourceAsStream("/ratings_db.txt");
        assertNotNull(stream);
        this.conn = new MockConnection(
                                new MockFileDatabase(stream
                                )
                    );
        assertNotNull(this.conn, "Connection is null; something has gone wrong with the fixture setup");
    }






    @Test
    public void bad_format_returns_501() throws Exception {

        final String testBody = "";
        ClobController controller = spy(new ClobController(new MetricRegistry()));
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        HashMap<String,Object> attributes = new HashMap<>();
        attributes.put(ContextUtil.maxRequestSizeKey,Integer.MAX_VALUE);

        when(request.getInputStream()).thenReturn(new TestServletInputStream(testBody));

        Context context = ContextUtil.init(request,response,"*",new HashMap<String,String>(), HandlerType.GET,attributes);
        context.attribute("database",this.conn);

        when(request.getAttribute("database")).thenReturn(this.conn);

        assertNotNull( context.attribute("database"), "could not get the connection back as an attribute");

        when(request.getHeader(Header.ACCEPT)).thenReturn("BAD FORMAT");

        assertThrows( FormattingException.class, () -> {
            controller.getAll(context);
        });

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

}
