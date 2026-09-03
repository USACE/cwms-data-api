package cwms.cda.data.dao;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

/**
 * A consumer for streaming binary data out to callers (e.g., HTTP layer).
 *
 * The primary abstract method accepts an InputStream with optional position and total length.
 * For generated content where the total length is not known up front (e.g., CSV rendered on-demand),
 * use the default two-argument overload which delegates with null position/length values, allowing
 * callers to choose chunked transfer without setting Content-Length.
 */
@FunctionalInterface
public interface StreamConsumer {

    void accept(InputStream stream, long inputStreamPosition, String mediaType, long totalLength) throws SQLException, IOException;

    default void accept(InputStream stream, String mediaType) throws SQLException, IOException {
        accept(stream, 0L, mediaType, -1L);
    }
}