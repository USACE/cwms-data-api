package mil.army.usace.hec;

import static io.restassured.RestAssured.*;
import static io.restassured.matcher.RestAssuredMatchers.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockFileDatabase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.restassured.RestAssured;


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
}
