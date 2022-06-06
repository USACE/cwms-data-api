package cwms.radar.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import com.codahale.metrics.MetricRegistry;
import cwms.radar.api.RatingController;
import cwms.radar.data.dao.DaoTest;
import cwms.radar.data.dao.JsonRatingUtils;
import cwms.radar.data.dao.JsonRatingUtilsTest;
import cwms.radar.formatters.Formats;
import cwms.radar.helpers.ResourceHelper;
import fixtures.TestServletInputStream;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import io.javalin.http.util.ContextUtil;
import io.javalin.plugin.json.JavalinJackson;
import io.javalin.plugin.json.JsonMapperKt;
import io.restassured.response.Response;
import org.jetbrains.annotations.Nullable;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockFileDatabase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import hec.data.cwmsRating.RatingSet;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class RatingsControllerTest {

    private static final Logger logger = Logger.getLogger(RatingsControllerTest.class.getName());

    private DataSource ds = mock(DataSource.class);
    private Connection conn = null;


    @BeforeEach
    public void baseLineDbMocks() throws IOException{
        InputStream stream = RatingsControllerTest.class.getResourceAsStream("/ratings_db.txt");
        assertNotNull(stream);
        this.conn = new MockConnection(new MockFileDatabase(stream));
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

        logger.log(Level.INFO, "Test post_to_create_passed_to_deserializeXml may trigger a RatingException - this is fine.");
        controller.create(context);
        // For this test, it's ok that the server throws a RatingException
        // Only want to check that the controller accessed our mock dao in the expected way
        verify(controller, times(1)).deserializeRatingSet(testBody, Formats.XML);  // Curious that it is XML and not XMLv2

    }

    @Disabled("incomplete")
    @Test
    void retrieve_create_retrieve_delete_retrieve_json() throws Exception{
        // Do whatever we need to do to startup the server
        int port = 7000;
        DataSource testDS = buildDataSource();
        String baseUri = "http://localhost:" + port;
        //// Todo: there needs to be some sort of Tomcat start here.
        try
        {
            // Read in a resource and build our test data
            String resourcePath = "cwms/radar/data/dao/BEAV.Stage_Flow.BASE.PRODUCTION.xml";
            String refRating = JsonRatingUtilsTest.loadResourceAsString(resourcePath);
            assertNotNull(refRating);
            RatingSet refRatingSet = RatingSet.fromXml(refRating);
            String office = "SWT";

            String refSpecId = refRatingSet.getName();
            String refRatingJson = JsonRatingUtils.toJson(refRatingSet);
            String newLoc = "TESTLOC";

            String testSpecId = refSpecId.replace("BEAV", newLoc);
            String testRatingJson = refRatingJson.replace("BEAV", newLoc).replace("Beaver", "TestLoc");

            try
            {
                // Make sure we can't find the new rating
                Response missingReponse = given()
                    .baseUri(baseUri)
                    .accept("application/json;version=2")
                    .param("office", office)
                    .when()
                    .get("/ratings/" + testSpecId)
                    .then()
                    .extract()
                    .response();
                Assertions.assertEquals(404, missingReponse.statusCode());
                // Cool, it's not there.

                // Now lets create it from json.
                Response createReponse = given()
                    .baseUri(baseUri)
                    .body(testRatingJson)
                    .accept("application/json;version=2")
                    .when()
                    .post("/ratings")
                    .then()
                    .extract()
                    .response();
                Assertions.assertEquals(200, createReponse.statusCode());
                // Cool, created it.

                // Now lets get it
                Response secondGetReponse = given()
                    .baseUri(baseUri)
                    .accept("application/json;version=2")
                    .param("office", office)
                    .when()
                    .get("/ratings/" + testSpecId)
                    .then()
                    .extract()
                    .response();
                Assertions.assertEquals(200, secondGetReponse.statusCode());
                // Cool, got it.
            }
            finally
            {
                // Now lets delete it
                Response deleteReponse = given()
                    .baseUri(baseUri)
                    .accept("application/json;version=2")
                    .param("office", office)
                    .when()
                    .delete("/ratings/" + testSpecId)
                    .then()
                    .extract()
                    .response();
                Assertions.assertEquals(200, deleteReponse.statusCode());
                // Cool its gone.
            }
        }finally {
            // Maybe somesort of Tomcat shutdown here?
        }
    }

    @Disabled("incomplete")
    @Test
    void retrieve_create_retrieve_delete_retrieve_xml() throws Exception{
        // Do whatever we need to do to startup the server
        int port = 7000;
        DataSource testDS = buildDataSource();
        String baseUri = "http://localhost:" + port;
        //// Todo: there needs to be some sort of Tomcat start here.
        try
        {
            // Read in a resource and build our test data
            String resourcePath = "cwms/radar/data/dao/BEAV.Stage_Flow.BASE.PRODUCTION.xml";
            String refRatingXml = JsonRatingUtilsTest.loadResourceAsString(resourcePath);
            assertNotNull(refRatingXml);
            RatingSet refRatingSet = RatingSet.fromXml(refRatingXml);
            String office = "SWT";

            String refSpecId = refRatingSet.getName();

            String newLoc = "TESTLOC";

            String testSpecId = refSpecId.replace("BEAV", newLoc);
            String testRatingXml = refRatingXml.replace("BEAV", newLoc).replace("Beaver", "TestLoc");

            try
            {
                // Make sure we can't find the new rating
                Response missingReponse = given()
                    .baseUri(baseUri)
                    .accept("application/xml;version=2")
                    .param("office",office)
                    .when()
                    .get("/ratings/" + testSpecId)
                    .then()
                    .extract()
                    .response();
                Assertions.assertEquals(404, missingReponse.statusCode());
                // Cool, it's not there.

                // Now lets create it from json.
                Response createReponse = given()
                    .baseUri(baseUri)
                    .body(testRatingXml)
                    .accept("application/xml;version=2")
                    .when()
                    .post("/ratings")
                    .then()
                    .extract()
                    .response();
                Assertions.assertEquals(200, createReponse.statusCode());
                // Cool, created it.

                // Now lets get it
                Response secondGetReponse = given()
                    .baseUri(baseUri)
                    .accept("application/xml;version=2")
                    .param("office", office)
                    .when()
                    .get("/ratings/" + testSpecId)
                    .then()
                    .extract()
                    .response();
                Assertions.assertEquals(200, secondGetReponse.statusCode());
                // Cool, got it.
            }
            finally
            {
                // Now lets delete it
                Response deleteReponse = given()
                    .baseUri(baseUri)
                    .accept("application/xml;version=2")
                    .param("office", office)
                    .when()
                    .delete("/ratings/" + testSpecId)
                    .then()
                    .extract()
                    .response();
                Assertions.assertEquals(200, deleteReponse.statusCode());
                // Cool its gone.
            }
        }finally {
            // Maybe somesort of Tomcat shutdown here?
        }
    }


    @Nullable
    private DataSource buildDataSource()
    {
        DataSource testDS = new DataSource()
        {
            @Override
            public Connection getConnection() throws SQLException
            {
                return DaoTest.getConnection();
            }

            @Override
            public Connection getConnection(String username, String password) throws SQLException
            {
                return DaoTest.getConnection();
            }

            @Override
            public <T> T unwrap(Class<T> iface) throws SQLException
            {
                return null;
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) throws SQLException
            {
                return false;
            }

            @Override
            public PrintWriter getLogWriter() throws SQLException
            {
                return null;
            }

            @Override
            public void setLogWriter(PrintWriter out) throws SQLException
            {

            }

            @Override
            public void setLoginTimeout(int seconds) throws SQLException
            {

            }

            @Override
            public int getLoginTimeout() throws SQLException
            {
                return 0;
            }

            @Override
            public Logger getParentLogger() throws SQLFeatureNotSupportedException
            {
                return null;
            }
        };
        return testDS;
    }


}
