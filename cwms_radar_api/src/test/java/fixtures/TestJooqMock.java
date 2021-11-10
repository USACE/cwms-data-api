package fixtures;

import java.io.InputStream;
import java.sql.SQLException;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;
import org.junit.jupiter.api.Test;

public class TestJooqMock {

    @Test
    public void test_json_mock_result() throws Exception{
        DSLContext dsl = DSL.using(SQLDialect.ORACLE12C);
        InputStream is = TestJooqMock.class.getResourceAsStream("/mock_location_test.json");

        Result<Record> record = dsl.fetchFromJSON(new String( is.readAllBytes() ));
        MockResult result = new MockResult(1,record);
        System.out.println(result.toString());
    }
}
