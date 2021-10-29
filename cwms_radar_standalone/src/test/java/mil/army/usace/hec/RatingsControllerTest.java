package mil.army.usace.hec;

import java.io.IOException;
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
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockFileDatabase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import hec.data.RatingException;

import static io.javalin.apibuilder.ApiBuilder.crud;
import static io.restassured.RestAssured.post;
import static org.mockito.ArgumentMatchers.eq;
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
        this.conn = new MockConnection(
                                new MockFileDatabase(
                                    RatingsControllerTest.class.getResourceAsStream("/ratings_db.txt")
                                )
                    );
        when(ds.getConnection()).thenReturn(conn);
    }

    @Test
    public void unimplemented_methods_return_501(){
        new RadarAPI(ds,7000).start();
        post("/ratings").then()
        .statusCode(HttpServletResponse.SC_NOT_IMPLEMENTED);
    }

    // This is only supposed to test that when XML data is posted to create,
    // that data is forward to the method deserializeFromXml
    @Test
    public void post_to_create_passed_to_deserializeXml() throws RatingException, IOException
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
        radarAPI.start();

        final String testBody = "could be anything";
        RequestSpecification request = RestAssured.given();
        Response response = request
                .config(RestAssuredConfig.config().encoderConfig(EncoderConfig.encoderConfig()
                        .appendDefaultContentCharsetToContentTypeIfUndefined(false)))
                .contentType(Formats.XMLV2)
                .body(testBody)
                .post("/ratings");

        // For this test, its ok that the server throws a RatingException
        // Only want to check that the controller accessed our mock dao in the expected way
        verify(controller, times(1)).
                deserializeRatingSet(eq(testBody), eq(Formats.XML));  // Curious that is is XML and not XMLv2
    }

}
