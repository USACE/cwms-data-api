package mil.army.usace.hec;

import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockFileDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.*;
import static io.restassured.matcher.RestAssuredMatchers.*;
import static org.hamcrest.Matchers.*;

import io.restassured.RestAssured;


public class ClobControllerTest {

    private DataSource ds = mock(DataSource.class);
    private Connection conn = null;
    private RadarAPI api = null;

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
        api = new RadarAPI(ds,7000);
        api.start();
    }

    @AfterEach
    public void stopAPI(){
        api.stop();
    }




    @Test
    public void bad_format_returns_501(){
        given()
        .header("Accept","Not_valid")
        .get("/clobs").then()
        .body("message",equalTo("failed to format data"))
        .statusCode(HttpServletResponse.SC_NOT_IMPLEMENTED);

    }

    @Test
    public void pagination_elements_returned_json(){
        given()
        .header("Accept","application/json;version=2").param("pageSize", 2)
        .get("/clobs")
        .then()
        .statusCode(HttpServletResponse.SC_OK)
        .body("$", hasKey("next-page"))
        .body("page-size", equalTo(2));

    }

    @Test
    public void pagination_elements_returned_xml(){
        given()
        .header("Accept","application/xml;version=2").param("pageSize", 2)
        .get("/clobs")
        .then()
        .statusCode(HttpServletResponse.SC_OK)
        .body("$", hasKey("nextPage"))
        .body("pageSize", equalTo(2));
    }
}
