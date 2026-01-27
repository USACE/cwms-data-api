package cwms.cda.data.dao;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

@FunctionalInterface
public interface StreamConsumer {
    void accept(InputStream stream, long inputStreamPosition, String mediaType, long totalLength) throws SQLException, IOException;
}