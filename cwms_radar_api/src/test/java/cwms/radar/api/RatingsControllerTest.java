package cwms.radar.api;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import cwms.radar.api.RatingController;
import cwms.radar.formatters.Formats;
import fixtures.TestServletInputStream;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import io.javalin.http.util.ContextUtil;
import io.javalin.plugin.json.JavalinJackson;
import io.javalin.plugin.json.JsonMapperKt;

import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockFileDatabase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.assertNotNull;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class RatingsControllerTest {

    private DataSource ds = mock(DataSource.class);
    private Connection conn = null;

    private static final MetricRegistry metrics = mock(MetricRegistry.class);
    private static final Meter meter = mock(Meter.class);
    private static final Timer timer = mock(Timer.class);



    @BeforeEach
    public void baseLineDbMocks() throws SQLException, IOException{
        InputStream stream = RatingsControllerTest.class.getResourceAsStream("/ratings_db.txt");
        assertNotNull(stream);
        this.conn = new MockConnection(
                                new MockFileDatabase(stream
                                )
                    );
        assertNotNull(this.conn, "Connection is null; something has gone wrong with the fixture setup");
    }


        // This is only supposed to test that when XML data is posted to create,
    // that data is forward to the method deserializeFromXml
    @Test
    void post_to_create_passed_to_deserializeXml() throws Exception
    {
        final String testBody = "could be anything";


        RatingController controller = spy(new RatingController(new MetricRegistry()));
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        HashMap<String,Object> attributes = new HashMap<>();
        attributes.put(ContextUtil.maxRequestSizeKey,Integer.MAX_VALUE);
        attributes.put(JsonMapperKt.JSON_MAPPER_KEY,new JavalinJackson());

        when(request.getInputStream()).thenReturn(new TestServletInputStream(testBody));
        //Context context = new Context(request,response, attributes);
        Context context = ContextUtil.init(request,response,"*",new HashMap<String,String>(), HandlerType.POST,attributes);
        context.attribute("database",this.conn);

        when(request.getContentLength()).thenReturn(testBody.length());
        when(request.getAttribute("database")).thenReturn(this.conn);

        assertNotNull( context.attribute("database"), "could not get the connection back as an attribute");

        when(request.getHeader(Header.ACCEPT)).thenReturn(Formats.XMLV2);
        when(request.getContentType()).thenReturn(Formats.XMLV2);


        controller.create(context);
        // For this test, it's ok that the server throws a RatingException
        // Only want to check that the controller accessed our mock dao in the expected way
        verify(controller, times(1)).deserializeRatingSet(testBody, Formats.XML);  // Curious that it is XML and not XMLv2

    }

}
