package cwms.cda.data.dao;

import static cwms.cda.data.dao.DaoTest.getDslContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.jooq.DSLContext;
import usace.cwms.db.jooq.codegen.packages.CWMS_TEXT_PACKAGE;

public class InsertClob {


    public static final String OFFICE = "SPK";
    String clobId = "/TIME SERIES TEXT/6261044";
    String filePath = "C:\\Temp\\out.log";  // just a biggish text file.

//    @Test  //not an actual test
    void test_insert() throws SQLException, FileNotFoundException {

        long millis = System.currentTimeMillis();

        DSLContext dsl = getDslContext(OFFICE);


        try {
            dsl.connection(c -> {
                CWMS_TEXT_PACKAGE.call_STORE_TEXT(
                        dsl.configuration(),
                        "asdf",
                        clobId,
                        "this is a clob for a test",
                        ClobDao.getBoolean(true),
                        OFFICE);
            });
        } catch (Exception e) {
            System.out.println("Exception - This is fine.  "
//                    + e.getMessage()
            );
            // Don't really care if it inserted or already existed.  Just needs to exist for this
            // next bit.
        }


        // Get a binary stream from filepath

        File file = new File(filePath);

        assert(file.exists());
        assert(file.length() > 1000);

        dsl.connection(connection -> {
            updateClobValue(connection, OFFICE, clobId, file);
        });


    }

//    @Test // This isn't a real test - just a way to load a bunch of data...
    void manual_connection() throws SQLException, ClassNotFoundException, IOException {

        String cdaJdbcUrl = System.getenv("CDA_JDBC_URL");
        if (cdaJdbcUrl == null) {
            cdaJdbcUrl = System.getProperty("CDA_JDBC_URL");
        }

        String username = System.getenv("CDA_JDBC_USERNAME");
        if (username == null) {
            username = System.getProperty("CDA_JDBC_USERNAME");
        }

        String password = System.getenv("CDA_JDBC_PASSWORD");
        if (password == null) {
            password = System.getProperty("CDA_JDBC_PASSWORD");
        }


        // File path and size
        File file = new File(filePath);
        System.out.println("File exists: " + file.exists() + " size: " + file.length());

        // Load the Oracle JDBC driver
        Class.forName("oracle.jdbc.driver.OracleDriver");

        // Establish the database connection
        Connection connection = DriverManager.getConnection(cdaJdbcUrl, username, password);

        updateClobValue(connection, OFFICE, clobId, file);  // 42Mb took 4.4s

        connection.close();
        }

    private static void updateClobValue(Connection connection, String office, String clobId,
                                        File file) throws SQLException, IOException {


        String sql = "UPDATE CWMS_20.AV_CLOB\n"
                + "SET VALUE = ?\n"
                + "where CWMS_20.AV_CLOB.ID = ?\n"
                + "  and CWMS_20.AV_CLOB.OFFICE_CODE in (select AV_OFFICE.OFFICE_CODE from CWMS_20.AV_OFFICE where AV_OFFICE.OFFICE_ID = ?)";

        Reader reader = new FileReader(file );

        try(PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setCharacterStream(1, reader);
            preparedStatement.setString(2, clobId);
            preparedStatement.setString(3, office);
            System.out.println("Executing statement: ");
            preparedStatement.executeUpdate();
        }

        System.out.println("CLOB data updated successfully with reader data.");
    }


}
