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
            + "WHERE CWMS_OFFICE.OFFICE_ID = ? and upper(ID) = upper(?)";
    public static final String BLOB_QUERY = "SELECT CWMS_MEDIA_TYPE.MEDIA_TYPE_ID, AT_BLOB.VALUE \n"
            + "FROM CWMS_20.AT_BLOB \n"
            + "join CWMS_20.CWMS_MEDIA_TYPE on AT_BLOB.MEDIA_TYPE_CODE = CWMS_MEDIA_TYPE.MEDIA_TYPE_CODE \n"
            + "WHERE upper(ID) = upper(?)";

    public BlobDao(DSLContext dsl) {
        super(dsl);
    }

    @Override
    public Optional<Blob> getByUniqueName(String id, String limitToOffice) {
        String queryStr = "SELECT AT_BLOB.ID, AT_BLOB.DESCRIPTION, CWMS_MEDIA_TYPE.MEDIA_TYPE_ID, CWMS_OFFICE.OFFICE_ID, AT_BLOB.VALUE \n"
                + "FROM CWMS_20.AT_BLOB \n"
                + "join CWMS_20.CWMS_MEDIA_TYPE on AT_BLOB.MEDIA_TYPE_CODE = CWMS_MEDIA_TYPE.MEDIA_TYPE_CODE \n"
                + "join CWMS_20.CWMS_OFFICE on AT_BLOB.OFFICE_CODE=CWMS_OFFICE.OFFICE_CODE \n"
                + "WHERE upper(ID) = upper(?)";
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

        connection(dsl, connection -> {
            try (PreparedStatement preparedStatement = connection.prepareStatement(BLOB_WITH_OFFICE)) {
                preparedStatement.setString(1, office);
                preparedStatement.setString(2, id);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        String mediaType = resultSet.getString("MEDIA_TYPE_ID");
                        java.sql.Blob blob = resultSet.getBlob("VALUE");
                        try {
                            consumer.accept(blob, mediaType);
                        } finally {
                            if (blob != null) {
                                blob.free();
                            }
                        }
                    } else {
                        throw new NotFoundException("Unable to find blob with id " + id + " in office " + office);
                    }
                }
            }
        });
    }

    public void getBlob(String id, BlobConsumer consumer) {

        connection(dsl, connection -> {
            try (PreparedStatement preparedStatement = connection.prepareStatement(BLOB_QUERY)) {
                preparedStatement.setString(1, id);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        String mediaType = resultSet.getString("MEDIA_TYPE_ID");
                        java.sql.Blob blob = resultSet.getBlob("VALUE");
                        try {
                            consumer.accept(blob, mediaType);
                        } finally {
                            if (blob != null) {
                                blob.free();
                            }
                        }
                    } else {
                        throw new NotFoundException("Unable to find blob with id " + id);
                    }
                }
            }
        });
    }

    public List<Blob> getAll(String officeId, String like) {
        String queryStr = "SELECT AT_BLOB.ID, AT_BLOB.DESCRIPTION, CWMS_MEDIA_TYPE.MEDIA_TYPE_ID, CWMS_OFFICE.OFFICE_ID\n"
                + " FROM CWMS_20.AT_BLOB \n"
                + "join CWMS_20.CWMS_MEDIA_TYPE on AT_BLOB.MEDIA_TYPE_CODE = CWMS_MEDIA_TYPE.MEDIA_TYPE_CODE \n"
                + "join CWMS_20.CWMS_OFFICE on AT_BLOB.OFFICE_CODE = CWMS_OFFICE.OFFICE_CODE \n"
                + " where REGEXP_LIKE (upper(AT_BLOB.ID), upper(?))";

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

        connection(dsl, c ->
                CWMS_TEXT_PACKAGE.call_STORE_BINARY(
                        getDslContext(c, blob.getOfficeId()).configuration(),
                        blob.getValue(),
                        blob.getId(),
                        blob.getMediaTypeId(),
                        blob.getDescription(),
                        pFailIfExists,
                        pIgnoreNulls,
                        blob.getOfficeId()));
    }

    public void update(Blob blob, boolean ignoreNulls) {
        String pFailIfExists = formatBool(false);
        String pIgnoreNulls = formatBool(ignoreNulls);

        if (blob == null) {
            throw new NotFoundException("Null blob provided to update");
        }

        if (!blobExists(blob.getOfficeId(), blob.getId())) {
            throw new NotFoundException("Unable to find blob with id " + blob.getId() + " in office " + blob.getOfficeId());
        }

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

    public void delete(String office, String id) {
        if (!blobExists(office, id)) {
            throw new NotFoundException("Unable to find blob with id " + id + " in office " + office);
        }
        dsl.connection(c -> CWMS_TEXT_PACKAGE.call_DELETE_BINARY(
                getDslContext(c, office).configuration(),
                id.toUpperCase(),
                office));
    }

    /**
     * Checks whether a blob exists for the given office and ID.
     * <p>
     * The ID is converted to uppercase during the comparison to ensure
     * case-insensitive matching.
     * </p>
     *
     * @param office the office associated with the blob
     * @param id     the unique identifier for the blob
     * @return true if the blob exists; false otherwise
     */
    private boolean blobExists(String office, String id) {
        String existsQuery = "select 1 "
                + "from CWMS_20.AT_BLOB \n"
                + "join CWMS_20.CWMS_OFFICE on AT_BLOB.OFFICE_CODE = CWMS_OFFICE.OFFICE_CODE \n"
                + "WHERE upper(CWMS_OFFICE.OFFICE_ID) = upper(?) AND upper(ID) = upper(?)";
        return connectionResult(dsl, conn -> {
            try (PreparedStatement preparedStatement = conn.prepareStatement(existsQuery)) {
                preparedStatement.setString(1, office);
                preparedStatement.setString(2, id);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    return resultSet.next();
                }
            }
        });
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
        void accept(java.sql.Blob blob, String mediaType) throws SQLException, IOException;
    }
}
