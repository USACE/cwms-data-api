package cwms.cda.datasource;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

public class ConnectionPreparingDataSource extends DelegatingDataSource {


    private ConnectionPreparer preparer;

    public ConnectionPreparingDataSource(ConnectionPreparer preparer, DataSource targetDataSource) {
        super(targetDataSource);
        this.preparer = preparer;
    }

    public ConnectionPreparingDataSource() {
    }

    @Override
    public Connection getConnection() throws SQLException {
        Connection connection = getDelegate().getConnection();
        return getPreparer().prepare(connection);
    }

    /**
     * @return the preparer
     */
    public ConnectionPreparer getPreparer() {
        return preparer;
    }

    /**
     * @param preparer the preparer to set
     */
    public void setPreparer(ConnectionPreparer preparer) {
        this.preparer = preparer;
    }


}