package cwms.cda.datasource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

public class DelegatingConnectionPreparer implements ConnectionPreparer {

    public static final Logger logger = Logger.getLogger(DelegatingConnectionPreparer.class.getName());
    private final List<ConnectionPreparer> delegates = new ArrayList<>();

    public DelegatingConnectionPreparer(List<ConnectionPreparer> preparers) {
        if (preparers != null) {
            delegates.addAll(preparers);
        }
    }

    public DelegatingConnectionPreparer(ConnectionPreparer... preparers) {
        for(ConnectionPreparer p : preparers) {
            Objects.requireNonNull(p,"A null prepared should not be passed to this function");
            delegates.add(p);
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
            logger.fine(delegate.getClass().getName());
            retval = delegate.prepare(retval);
        }

        return retval;
    }

}
