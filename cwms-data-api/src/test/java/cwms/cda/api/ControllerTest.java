package cwms.cda.api;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Scanner;
import org.jetbrains.annotations.NotNull;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockFileDatabase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.owasp.html.HtmlPolicyBuilder;
import org.owasp.html.PolicyFactory;

@TestInstance(Lifecycle.PER_CLASS)
public class ControllerTest {
    protected Connection conn = null;
    protected PolicyFactory sanitizer =
            new HtmlPolicyBuilder().disallowElements("<script>").toFactory();

    public Connection getTestConnection() throws IOException {
        if (conn == null) {
            InputStream stream = ControllerTest.class.getResourceAsStream("/ratings_db.txt");
            assertNotNull(stream);
            this.conn = new MockConnection(
                    new MockFileDatabase(stream
                    )
            );
            assertNotNull(this.conn, "Connection is null; something has gone wrong with the "
                    + "fixture setup");
        }
        return conn;
    }

    public String loadResourceAsString(String fileName) {
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream stream = classLoader.getResourceAsStream(fileName);
        assertNotNull(stream, "Could not load the resource as stream:" + fileName);
        Scanner scanner = new Scanner(stream);
        String contents = scanner.useDelimiter("\\A").next();
        scanner.close();
        return contents;
    }

    @BeforeAll
    public void baseLineDbMocks() throws IOException {
        InputStream stream = ControllerTest.class.getResourceAsStream("/ratings_db.txt");
        assertNotNull(stream);
        this.conn = new MockConnection(
                new MockFileDatabase(stream
                )
        );
        assertNotNull(this.conn, "Connection is null; something has gone wrong with the fixture "
                + "setup");
    }

    @NotNull
    public static String buildParamStr(Map<String, String> urlParams) {
        StringBuilder sb = new StringBuilder();
        urlParams.entrySet()
                .forEach(e -> sb.append(e.getKey()).append("=").append(e.getValue()).append("&"));

        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }

        return sb.toString();
    }
}
