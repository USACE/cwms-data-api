package cwms.cda.data.dao;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.RangeParser;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.Blob;
import cwms.cda.data.dto.Blobs;
import cwms.cda.data.dto.CwmsDTOPaginated;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record4;
import org.jooq.ResultQuery;
import org.jooq.SelectLimitPercentStep;
import org.jooq.Table;

import usace.cwms.db.jooq.codegen.packages.CWMS_TEXT_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_CWMS_MEDIA_TYPE;
import usace.cwms.db.jooq.codegen.tables.AV_OFFICE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.noCondition;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.upper;

public class BlobDao extends JooqDao<Blob> implements BlobAccess {
    static FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String ID = "ID";
    public static final String DESCRIPTION = "DESCRIPTION";
    public static final String OFFICE_CODE = "OFFICE_CODE";
    public static final String MEDIA_TYPE_CODE = "MEDIA_TYPE_CODE";
    public static final String MEDIA_TYPE_ID = "MEDIA_TYPE_ID";
    public static final String VALUE = "VALUE";
    public static final String OFFICE_ID = "OFFICE_ID";

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
            String rId = r.get(ID, String.class);
            String rOffice = r.get(OFFICE_ID, String.class);
            String rDesc = r.get(DESCRIPTION, String.class);
            String rMedia = r.get(MEDIA_TYPE_ID, String.class);
            byte[] value = r.get(VALUE, byte[].class);
            return new Blob(rOffice, rId, rDesc, rMedia, value);
        });

        return Optional.ofNullable(retVal);
    }

    @Override
    public void getBlob(String id, String office, StreamConsumer consumer, @Nullable Long offset, @Nullable Long end) {

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

            if(office == null ){
                try (PreparedStatement preparedStatement = connection.prepareStatement(BLOB_QUERY)) {
                    preparedStatement.setString(1, id);
                    executeAndHandle(consumer, offset, end, preparedStatement, "Unable to find blob with id " + id);
                }
            } else {
                try (PreparedStatement preparedStatement = connection.prepareStatement(BLOB_WITH_OFFICE)) {
                    preparedStatement.setString(1, office);
                    preparedStatement.setString(2, id);

                    executeAndHandle(consumer, offset, end, preparedStatement, "Unable to find blob with id " + id + " in office " + office);
                }
            }
        });
    }

    private static void executeAndHandle(StreamConsumer consumer, @Nullable Long offset, @Nullable Long end, PreparedStatement preparedStatement, String message) throws SQLException, IOException {
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                handleResultSet(consumer, offset, end, resultSet);
            } else {
                throw new NotFoundException(message);
            }
        }
    }

    /**
     *
     * @param consumer
     * @param offset where to start reading.  0 is first byte
     * @param end position of last byte to include. inclusive.  0 would mean only return the first byte.
     * @param resultSet
     * @throws SQLException
     * @throws IOException
     */
    private static void handleResultSet(StreamConsumer consumer, @Nullable Long offset, @Nullable Long end, ResultSet resultSet) throws SQLException, IOException {
        String mediaType = resultSet.getString("MEDIA_TYPE_ID");
        java.sql.Blob blob = resultSet.getBlob("VALUE");
        try {
            long totalLength = blob.length();
            if (offset != null) {
                long pos = offset + 1; // For getBinaryStream the first byte is at 1.
                long length = getLength(offset, end, totalLength);

                logger.atFine().log("Reading blob at pos %s, length %s, totalLength %s", pos, length, totalLength);
                try (InputStream stream = blob.getBinaryStream(pos, length)) {
                    consumer.accept(stream, offset, mediaType, totalLength);
                } catch (SQLException e) {
                    logger.atWarning().withCause(e).log("Error reading blob at offset %s, length %s, totalLength %s", offset, length, totalLength);
                    throw e;
                }
            } else {
                try (InputStream stream = blob.getBinaryStream()) {
                    consumer.accept(stream, 0, mediaType, totalLength);
                }
            }
        } finally {
            blob.free();
        }
    }

    /**
     *
     * @param offset the index of the first byte to read. Like http range-requests 0 is first byte. -1 is last byte.
     * @param end the index of the last byte to read, inclusive. null reads until the end of the blob. -1 is also last byte.
     *
     * @param totalLength the total length of the blob
     * @return the length of the range to read
     */
    static long getLength(@NotNull Long offset, @Nullable Long end, long totalLength) {

        long length;
        if(end != null){
            // The length we are getting passed in is from range-request and could be negative to indicate suffix
            long[] startEnd = RangeParser.interpret(new long[]{offset, end}, totalLength);
            if(startEnd != null){
                offset = startEnd[0];
                end = startEnd[1];
            }

            length = end - offset + 1;
        } else {
            // if its not set just assume we are reading until the end of blob.
            // Consumer can always close stream early.
            length = totalLength - offset;
        }
        return length;
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
            String rId = r.get(ID, String.class);
            String rOffice = r.get(OFFICE_ID, String.class);
            String rDesc = r.get(DESCRIPTION, String.class);
            String rMedia = r.get(MEDIA_TYPE_ID, String.class);

            return new Blob(rOffice, rId, rDesc, rMedia, null);
        });
    }

    /**
     * Retrieves all blobs with pagination support.
     *
     * @param cursor   the pagination cursor, can be null or empty for the first page
     * @param pageSize the number of blobs to retrieve per page
     * @param officeId filter by office ID, can be null or empty to include all offices
     * @param like     filter blobs by a case-insensitive regex pattern on their IDs, can be null or empty
     * @return a Blobs object containing the retrieved blobs and pagination information
     */
    @Override
    public @NotNull Blobs getBlobs(@Nullable String cursor, int pageSize, @Nullable String officeId, @Nullable String like) {

        String cursorOffice = null;
        String cursorId = null;

        AV_CWMS_MEDIA_TYPE cwmsMediaType = AV_CWMS_MEDIA_TYPE.AV_CWMS_MEDIA_TYPE.as("cmt");
        AV_OFFICE vOffice = AV_OFFICE.AV_OFFICE.as("vo");

        // 2025-07-28 AT_BLOB does not seem to be in the codegen but I'd still like to use the DSL style.
        // Manually create the blob table and fields.
        Table<?> atBlob = table(name("CWMS_20", "AT_BLOB")).as("bt");
        Field<String> blobIdFld = field(name(atBlob.getName(), ID), String.class);
        Field<String> descFld = field(name(atBlob.getName(), DESCRIPTION), String.class);
        Field<Long> officeCodeFld = field(name(atBlob.getName(), OFFICE_CODE), Long.class);
        Field<Long> mediaCodeFld = field(name(atBlob.getName(), MEDIA_TYPE_CODE), Long.class);

        Condition pagingCondition = noCondition();
        if (cursor != null && !cursor.isEmpty()) {
            final String[] parts = CwmsDTOPaginated.decodeCursor(cursor, "||");

            if (parts.length > 1) {
                cursorOffice = Blobs.getOffice(cursor);
                cursorId = Blobs.getId(cursor);

                pageSize = Integer.parseInt(parts[2]);
            }

            Condition moreInSameOffice = cursorId == null || cursorOffice == null ? noCondition() :
                    vOffice.OFFICE_ID.eq(cursorOffice.toUpperCase())
                            .and(upper(blobIdFld).greaterThan(cursorId.toUpperCase()));
            Condition nextOffices = cursorOffice == null ? noCondition():
                    upper(vOffice.OFFICE_ID).greaterThan(cursorOffice.toUpperCase());
            pagingCondition = moreInSameOffice.or(nextOffices);
        }

        Condition whereCondition = noCondition();

        if (like != null && !like.isEmpty()) {
            whereCondition = whereCondition.and(caseInsensitiveLikeRegex(blobIdFld, like));
        }
        if(officeId != null && !officeId.isEmpty()) {
            whereCondition = whereCondition.and(upper(vOffice.OFFICE_ID).eq(upper(officeId)));
        }

        SelectLimitPercentStep<Record4<String, String, String, String>> query = dsl.select(blobIdFld, descFld, cwmsMediaType.MEDIA_TYPE_ID, vOffice.OFFICE_ID)
                .from(atBlob)
                .join(cwmsMediaType).on(mediaCodeFld.eq(cwmsMediaType.MEDIA_TYPE_CODE.cast(Long.class)))
                .join(vOffice).on(vOffice.OFFICE_CODE.eq(officeCodeFld))
                .where(whereCondition)
                .and(pagingCondition)
                .orderBy(vOffice.OFFICE_ID, blobIdFld)
                .limit(pageSize);

        Blobs.Builder builder = new Blobs.Builder(cursor, pageSize, 0);

        try (Stream<Record4<String, String, String, String>> stream = query.stream()){
            stream.forEach(r -> {
                String rId = r.value1();
                String rDesc = r.value2();
                String rMedia = r.value3();
                String rOffice = r.value4();

                Blob blob = new Blob(rOffice, rId, rDesc, rMedia, null);
                builder.addBlob(blob);
            });
        }

        return builder.build();
    }

    @Override
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

    @Override
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

    @Override
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


}
