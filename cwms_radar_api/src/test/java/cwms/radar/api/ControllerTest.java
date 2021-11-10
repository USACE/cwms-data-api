package cwms.radar.api;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Scanner;

import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockFileDatabase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestInstance(Lifecycle.PER_CLASS)
public class ControllerTest
{
    protected Connection conn = null;
    public Connection getTestConnection() throws SQLException, IOException{
        if( conn == null ){
            InputStream stream = ControllerTest.class.getResourceAsStream("/ratings_db.txt");
            assertNotNull(stream);
            this.conn = new MockConnection(
                                    new MockFileDatabase(stream
                                    )
                        );
            assertNotNull(this.conn, "Connection is null; something has gone wrong with the fixture setup");
        }
        return conn;
    }
    public String loadResourceAsString(String fileName)
    {
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream stream = classLoader.getResourceAsStream(fileName);
        assertNotNull(stream, "Could not load the resource as stream:" + fileName);
        Scanner scanner = new Scanner(stream);
        String contents = scanner.useDelimiter("\\A").next();
        scanner.close();
        return contents;
    }

    @BeforeAll
    public void baseLineDbMocks() throws SQLException, IOException{
        InputStream stream = ControllerTest.class.getResourceAsStream("/ratings_db.txt");
        assertNotNull(stream);
        this.conn = new MockConnection(
                                new MockFileDatabase(stream
                                )
                    );
        assertNotNull(this.conn, "Connection is null; something has gone wrong with the fixture setup");
    }
}
