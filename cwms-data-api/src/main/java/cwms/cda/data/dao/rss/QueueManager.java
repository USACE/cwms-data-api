/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dao.rss;

import com.google.common.flogger.FluentLogger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

public final class QueueManager {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final String SUBSCRIBER_NAME = "CDA_QUEUE_SUBSCRIBER";
    private static final String SQL = "BEGIN " +
        "   FOR rec IN ( " +
        "      SELECT OWNER, NAME " +
        "      FROM ALL_QUEUES " +
        "      WHERE OWNER = 'CWMS_20' " +
        "        AND QUEUE_TYPE = 'NORMAL_QUEUE' " +
        "        AND REGEXP_LIKE(NAME, 'STATUS|TS_UPDATES|REALTIME_OPS') " +
        "   ) LOOP " +
        "      BEGIN " +
        "         DBMS_AQADM.ADD_SUBSCRIBER( " +
        "            queue_name => rec.OWNER || '.' || rec.NAME, " +
        "            subscriber => sys.aq$_agent(?, NULL, NULL) " +
        "         ); " +
        "      EXCEPTION " +
        "         WHEN OTHERS THEN " +
        "            IF SQLCODE != -24034 THEN RAISE; END IF; " + // Ignore "Already a subscriber"
        "      END; " +
        "   END LOOP; " +
        "   COMMIT;" +
        "END;";

    private QueueManager() {
        throw new AssertionError("Utility class");
    }

    public static void ensureRssSubscribers(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(SQL)) {
            stmt.setString(1, SUBSCRIBER_NAME);
            stmt.execute();
        } catch (SQLException ex) {
            LOGGER.atWarning().withCause(ex).log("Unable to ensure CDA persists AQ subscriptions. " +
                "RSS endpoints may miss events.");
        }
    }
}
