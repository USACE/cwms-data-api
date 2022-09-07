package cwms.radar.datasource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DelegatingConnectionPreparer implements ConnectionPreparer {

    private final List<ConnectionPreparer> delegates = new ArrayList<>();

    public DelegatingConnectionPreparer(List<ConnectionPreparer> preparers) {
        if (preparers != null) {
            delegates.addAll(preparers);
        }
    }

    public DelegatingConnectionPreparer(ConnectionPreparer prep1, ConnectionPreparer prep2) {
        if (prep1 != null) {
            delegates.add(prep1);
        }
        if (prep2 != null) {
            delegates.add(prep2);
        }
    }

    @Override
    public Connection prepare(Connection connection) throws SQLException {
        Connection retval = connection;
        for (ConnectionPreparer delegate : delegates) {
            retval = delegate.prepare(retval);
        }

        return retval;
    }

}
