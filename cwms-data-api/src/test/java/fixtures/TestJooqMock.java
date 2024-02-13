package fixtures;

import java.io.InputStream;
import java.util.Scanner;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.Test;

public class TestJooqMock {

    @Test
    public void test_json_mock_result() throws Exception {
        DSLContext dsl = DSL.using(SQLDialect.ORACLE18C);
        InputStream is = TestJooqMock.class.getResourceAsStream("/mock_location_test.json");
        Scanner scanner = new Scanner(is);
        String contents = scanner.useDelimiter("\\A").next();
        scanner.close();
        Result<Record> record = dsl.fetchFromJSON(contents);
        MockResult result = new MockResult(1,record);
    }
}
