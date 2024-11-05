package cwms.cda.data.dao;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.Blob;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.ResultQuery;
import usace.cwms.db.jooq.codegen.packages.CWMS_TEXT_PACKAGE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class BlobDao extends JooqDao<Blob> {

    public static final String BLOB_WITH_OFFICE = "SELECT CWMS_MEDIA_TYPE.MEDIA_TYPE_ID, AT_BLOB.VALUE \n"
            + "FROM CWMS_20.AT_BLOB \n"
            + "join CWMS_20.CWMS_MEDIA_TYPE on AT_BLOB.MEDIA_TYPE_CODE = CWMS_MEDIA_TYPE.MEDIA_TYPE_CODE \n"
            + "join CWMS_20.CWMS_OFFICE on AT_BLOB.OFFICE_CODE=CWMS_OFFICE.OFFICE_CODE \n"
            + "WHERE ID = ? and CWMS_OFFICE.OFFICE_ID = ?";
    public static final String BLOB_QUERY = "SELECT CWMS_MEDIA_TYPE.MEDIA_TYPE_ID, AT_BLOB.VALUE \n"
            + "FROM CWMS_20.AT_BLOB \n"
            + "join CWMS_20.CWMS_MEDIA_TYPE on AT_BLOB.MEDIA_TYPE_CODE = CWMS_MEDIA_TYPE.MEDIA_TYPE_CODE \n"
            + "WHERE ID = ?";

    public BlobDao(DSLContext dsl) {
        super(dsl);
    }

    @Override
    public Optional<Blob> getByUniqueName(String id, String limitToOffice) {
        String queryStr = "SELECT AT_BLOB.ID, AT_BLOB.DESCRIPTION, CWMS_MEDIA_TYPE.MEDIA_TYPE_ID, CWMS_OFFICE.OFFICE_ID, AT_BLOB.VALUE \n"
                + "FROM CWMS_20.AT_BLOB \n"
                + "join CWMS_20.CWMS_MEDIA_TYPE on AT_BLOB.MEDIA_TYPE_CODE = CWMS_MEDIA_TYPE.MEDIA_TYPE_CODE \n"
                + "join CWMS_20.CWMS_OFFICE on AT_BLOB.OFFICE_CODE=CWMS_OFFICE.OFFICE_CODE \n"
                + "WHERE ID = ?";
        ResultQuery<Record> query;
        if (limitToOffice != null && !limitToOffice.isEmpty()) {
            queryStr = queryStr + " and CWMS_OFFICE.OFFICE_ID = ?";
            query = dsl.resultQuery(queryStr, id, limitToOffice);
        } else {
            query = dsl.resultQuery(queryStr, id);
        }

        Blob retVal = query.fetchOne(r -> {
            String rId = r.get("ID", String.class);
            String rOffice = r.get("OFFICE_ID", String.class);
            String rDesc = r.get("DESCRIPTION", String.class);
            String rMedia = r.get("MEDIA_TYPE_ID", String.class);
            byte[] value = r.get("VALUE", byte[].class);
            return new Blob(rOffice, rId, rDesc, rMedia, value);
        });

        return Optional.ofNullable(retVal);
    }

    public void getBlob(String id, String office, BlobConsumer consumer) {
        // Not using jOOQ here because we want the java.sql.Blob and not an automatic field binding.  We want
        // blob so that we can pull out a stream to the data and pass that to javalin.
        // If the request included Content-Ranges Javalin can have the stream skip to the correct
        // location, which will avoid reading unneeded data.  Passing this stream right to the javalin
        // response should let CDA return a huge blob to the client without ever holding the entire byte[]
        // in memory.
        // We can't use the stream once the connection we get from jooq is closed, so we have to pass in
        // what we want javalin to do with the stream as a consumer.
        //

        dsl.connection(connection -> {
            try (PreparedStatement preparedStatement = connection.prepareStatement(BLOB_WITH_OFFICE)) {
                preparedStatement.setString(1, id);
                preparedStatement.setString(2, office);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        handleResultSet(resultSet, consumer);
                    } else {
                        throw new NotFoundException("Unable to find blob with id " + id + " in office " + office);
                    }
                }
            }
        });
    }

    public void getBlob(String id, BlobConsumer consumer) {

        dsl.connection(connection -> {
            try (PreparedStatement preparedStatement = connection.prepareStatement(BLOB_QUERY)) {
                preparedStatement.setString(1, id);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        handleResultSet(resultSet, consumer);
                    } else {
                        throw new NotFoundException("Unable to find blob with id " + id);
                    }
                }
            }
        });
    }

    private static void handleResultSet(ResultSet resultSet, BlobConsumer consumer) throws SQLException {
        String mediaType = resultSet.getString("MEDIA_TYPE_ID");
        java.sql.Blob blob = resultSet.getBlob("VALUE");
        consumer.accept(blob, mediaType);
    }


    @Override
    public List<Blob> getAll(String officeId) {
        String queryStr = "SELECT AT_BLOB.ID, AT_BLOB.DESCRIPTION, CWMS_MEDIA_TYPE.MEDIA_TYPE_ID, CWMS_OFFICE.OFFICE_ID\n"
                + " FROM CWMS_20.AT_BLOB \n"
                + "join CWMS_20.CWMS_MEDIA_TYPE on AT_BLOB.MEDIA_TYPE_CODE = CWMS_MEDIA_TYPE.MEDIA_TYPE_CODE \n"
                + "join CWMS_20.CWMS_OFFICE on AT_BLOB.OFFICE_CODE=CWMS_OFFICE.OFFICE_CODE \n"
                ;

        ResultQuery<Record> query;
        if (officeId != null) {
            queryStr = queryStr + " and upper(CWMS_OFFICE.OFFICE_ID) = upper(?)";
            query = dsl.resultQuery(queryStr, officeId);
        } else {
            query = dsl.resultQuery(queryStr);
        }

        return query.fetch(r -> {
            String rId = r.get("ID", String.class);
            String rOffice = r.get("OFFICE_ID", String.class);
            String rDesc = r.get("DESCRIPTION", String.class);
            String rMedia = r.get("MEDIA_TYPE_ID", String.class);

            return new Blob(rOffice, rId, rDesc, rMedia, null);
        });
    }

    public List<Blob> getAll(String  officeId, String like) {
        String queryStr = "SELECT AT_BLOB.ID, AT_BLOB.DESCRIPTION, CWMS_MEDIA_TYPE.MEDIA_TYPE_ID, CWMS_OFFICE.OFFICE_ID\n"
                + " FROM CWMS_20.AT_BLOB \n"
                + "join CWMS_20.CWMS_MEDIA_TYPE on AT_BLOB.MEDIA_TYPE_CODE = CWMS_MEDIA_TYPE.MEDIA_TYPE_CODE \n"
                + "join CWMS_20.CWMS_OFFICE on AT_BLOB.OFFICE_CODE=CWMS_OFFICE.OFFICE_CODE \n"
                + " where REGEXP_LIKE (upper(AT_BLOB.ID), upper(?))"
                ;

        ResultQuery<Record> query;
        if (officeId != null) {
            queryStr = queryStr + " and upper(CWMS_OFFICE.OFFICE_ID) = upper(?)";
            query = dsl.resultQuery(queryStr, like, officeId);
        } else {
            query = dsl.resultQuery(queryStr, like);
        }

        return query.fetch(r -> {
            String rId = r.get("ID", String.class);
            String rOffice = r.get("OFFICE_ID", String.class);
            String rDesc = r.get("DESCRIPTION", String.class);
            String rMedia = r.get("MEDIA_TYPE_ID", String.class);

            return new Blob(rOffice, rId, rDesc, rMedia, null);
        });
    }

    public void create(Blob blob, boolean failIfExists, boolean ignoreNulls) {
        String pFailIfExists = formatBool(failIfExists);
        String pIgnoreNulls = formatBool(ignoreNulls);
        dsl.connection(c -> CWMS_TEXT_PACKAGE.call_STORE_BINARY(
                getDslContext(c, blob.getOfficeId()).configuration(),
                blob.getValue(),
                blob.getId(),
                blob.getMediaTypeId(),
                blob.getDescription(),
                pFailIfExists,
                pIgnoreNulls,
                blob.getOfficeId()));
    }


    public static byte[] readFully(@NotNull InputStream stream) throws IOException {
        byte[] buffer = new byte[8192];
        int bytesRead;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        while ((bytesRead = stream.read(buffer)) != -1) {
            output.write(buffer, 0, bytesRead);
        }
        return output.toByteArray();
    }

    @FunctionalInterface
    public interface BlobConsumer {
        void accept(java.sql.Blob blob, String mediaType) throws SQLException;
    }
}
