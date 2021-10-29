package mil.army.usace.hec;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import com.codahale.metrics.MetricRegistry;
import cwms.radar.api.RatingController;
import cwms.radar.formatters.Formats;
import io.restassured.RestAssured;
import io.restassured.config.EncoderConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.specification.RequestSpecification;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockFileDatabase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import hec.data.RatingException;

import static io.javalin.apibuilder.ApiBuilder.crud;
import static io.restassured.RestAssured.post;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class RatingsControllerTest {

    private DataSource ds = mock(DataSource.class);
    private Connection conn = null;

    @BeforeAll
    public static void startServer(){

        RestAssured.port = 7000;
    }

    @BeforeEach
    public void baseLineDbMocks() throws SQLException, IOException{
        InputStream stream = RatingsControllerTest.class.getResourceAsStream("/ratings_db.txt");
        assertNotNull(stream);
        this.conn = new MockConnection(
                                new MockFileDatabase(stream
                                )
                    );
        when(ds.getConnection()).thenReturn(conn);
    }

    @Test
    void unimplemented_methods_return_501(){
        RadarAPI radarAPI = new RadarAPI(ds, 7000);
        try
        {
            radarAPI.start();
            post("/ratings").then().statusCode(HttpServletResponse.SC_NOT_IMPLEMENTED);
        }finally
        {
            radarAPI.stop();
        }
    }

    // This is only supposed to test that when XML data is posted to create,
    // that data is forward to the method deserializeFromXml
    @Test
    void post_to_create_passed_to_deserializeXml() throws RatingException, IOException
    {
        RatingController controller = spy(new RatingController(new MetricRegistry()));

        class MyRadar extends RadarAPI {
            public MyRadar(DataSource ds, int port)
            {
                super(ds, port);
            }

            @Override
            protected void configureRoutes()
            {
                crud("/ratings/{rating}", controller);
            }
        }
        MyRadar radarAPI = new MyRadar(ds, 7000);

        try
        {
            radarAPI.start();

            final String testBody = "could be anything";
            RequestSpecification request = RestAssured.given();
            request.config(RestAssuredConfig.config().encoderConfig(
                    EncoderConfig.encoderConfig().appendDefaultContentCharsetToContentTypeIfUndefined(false))).contentType(Formats.XMLV2).body(testBody).post("/ratings");

            // For this test, it's ok that the server throws a RatingException
            // Only want to check that the controller accessed our mock dao in the expected way
            verify(controller, times(1)).deserializeRatingSet(testBody, Formats.XML);  // Curious that it is XML and not XMLv2
        } finally
        {
            radarAPI.stop();
        }
    }

}
