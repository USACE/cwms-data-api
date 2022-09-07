package cwms.radar.datasource;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionPreparer {
    Connection prepare(Connection connection) throws SQLException;
}
