package cwms.cda.data.dao;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.Clob;
import cwms.cda.data.dto.Clobs;
import cwms.cda.data.dto.CwmsDTOPaginated;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record4;
import org.jooq.RecordMapper;
import org.jooq.SelectConditionStep;
import org.jooq.SelectLimitPercentStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_TEXT_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_CLOB;
import usace.cwms.db.jooq.codegen.tables.AV_OFFICE;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static org.jooq.impl.DSL.*;

public class ClobDao extends JooqDao<Clob> {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String SELECT_CLOB_QUERY = "select cwms_20.AV_CLOB.VALUE "
            + "from cwms_20.av_clob join cwms_20.av_office "
            + "on av_clob.office_code = av_office.office_code "
            + "where av_office.office_id = ? and av_clob.id = ?";

    public ClobDao(DSLContext dsl) {
        super(dsl);
    }


    // Yikes, I hate this method - it retrieves all the clobs?  That could be gigabytes of data.
    // Not returning Value or Desc fields until a useful way of working with this method is
    // figured out.
    @Override
    public List<Clob> getAll(String limitToOffice) {
        AV_CLOB ac = AV_CLOB.AV_CLOB;
        AV_OFFICE ao = AV_OFFICE.AV_OFFICE;

        Condition whereCond = noCondition();
        if (limitToOffice != null && !limitToOffice.isEmpty()) {
            whereCond = ao.OFFICE_ID.eq(limitToOffice);
        }

        return dsl.select(ac.ID, ao.OFFICE_ID)
                .from(ac.join(ao).on(ac.OFFICE_CODE.eq(ao.OFFICE_CODE)))
                .where(whereCond)
                .fetch(joinRecord ->
                        new Clob(joinRecord.get(ao.OFFICE_ID),
                                joinRecord.get(ac.ID), null, null));

    }

    @Override
    public Optional<Clob> getByUniqueName(String uniqueName, String office) {
        AV_CLOB ac = AV_CLOB.AV_CLOB;
        AV_OFFICE ao = AV_OFFICE.AV_OFFICE;

        Condition cond = ac.ID.eq(uniqueName);
        if (office != null && !office.isEmpty()) {
            cond = cond.and(ao.OFFICE_ID.eq(office));
        }

        RecordMapper<Record, Clob> mapper = joinRecord ->
                new Clob(joinRecord.getValue(ao.OFFICE_ID),
                        joinRecord.getValue(ac.ID),
                        joinRecord.getValue(ac.DESCRIPTION),
                        joinRecord.getValue(ac.VALUE)
                );

        return dsl.select(ao.OFFICE_ID, ac.asterisk())
                .from(ac.join(ao).on(ac.OFFICE_CODE.eq(ao.OFFICE_CODE)))
                .where(cond)
                .fetchOptional(mapper);
    }

    public Clobs getClobs(String cursor, int pageSize, String officeLike,
                          boolean includeValues) {
        return getClobs(cursor, pageSize, officeLike, includeValues, ".*");
    }

    public Clobs getClobs(String cursor, int pageSize, String officeLike,
                          boolean includeValues, String idRegex) {
        int total = 0;
        String clobCursor = "*";
        AV_CLOB v_clob = AV_CLOB.AV_CLOB;
        AV_OFFICE v_office = AV_OFFICE.AV_OFFICE;

        if (cursor == null || cursor.isEmpty()) {

            SelectConditionStep<Record1<Integer>> count =
                    dsl.select(count(asterisk()))
                            .from(v_clob)
                            .join(v_office).on(v_clob.OFFICE_CODE.eq(v_office.OFFICE_CODE))
                            .where(JooqDao.caseInsensitiveLikeRegex(v_clob.ID, idRegex))
                            .and(officeLike == null ? noCondition() : v_office.OFFICE_ID.like(officeLike.toUpperCase()));

            total = count.fetchOne().value1();
        } else {
            final String[] parts = CwmsDTOPaginated.decodeCursor(cursor, "||");

            logger.atFine().log("decoded cursor: " + String.join("||", parts));
            for (String p : parts) {
                logger.atFinest().log(p);
            }

            if (parts.length > 1) {
                clobCursor = parts[0].split(";")[0];
                clobCursor = clobCursor.substring(clobCursor.indexOf("/") + 1); // ditch the
                // officeId that's embedded in
                total = Integer.parseInt(parts[1]);
                pageSize = Integer.parseInt(parts[2]);
            }
        }

        SelectLimitPercentStep<Record4<String, String, String, String>> query = dsl.select(
                        v_office.OFFICE_ID,
                        v_clob.ID,
                        v_clob.DESCRIPTION,
                        includeValues ? v_clob.VALUE : DSL.inline("").as(v_clob.VALUE)
                )
                .from(v_clob)
                //.innerJoin(forLimit).on(forLimit.field(v_clob.ID).eq(v_clob.ID))
                .join(v_office).on(v_clob.OFFICE_CODE.eq(v_office.OFFICE_CODE))
                .where(JooqDao.caseInsensitiveLikeRegex(v_clob.ID,idRegex))
                .and(DSL.upper(v_clob.ID).greaterThan(clobCursor))
                .orderBy(v_clob.ID).limit(pageSize);


        Clobs.Builder builder = new Clobs.Builder(clobCursor, pageSize, total);

        logger.atFine().log(query.getSQL(ParamType.INLINED));

        query.fetch().forEach(row -> {
            usace.cwms.db.jooq.codegen.tables.records.AV_CLOB clob = row.into(v_clob);
            usace.cwms.db.jooq.codegen.tables.records.AV_OFFICE clobOffice = row.into(v_office);
            builder.addClob(new Clob(
                    clobOffice.getOFFICE_ID(),
                    clob.getID(),
                    clob.getDESCRIPTION(),
                    clob.getVALUE()
            ));

        });

        return builder.build();
    }


    public List<Clob> getClobsLike(String office, String idLike) {
        AV_CLOB ac = AV_CLOB.AV_CLOB;
        AV_OFFICE ao = AV_OFFICE.AV_OFFICE;

        Condition cond = DSL.upper(ac.ID).like(idLike.toUpperCase());
        if (office != null && !office.isEmpty()) {
            cond = cond.and(ao.OFFICE_ID.eq(office.toUpperCase()));
        }

        RecordMapper<Record, Clob> mapper = joinRecord ->
                new Clob(joinRecord.get(ao.OFFICE_ID),
                        joinRecord.get(ac.ID),
                        joinRecord.get(ac.DESCRIPTION),
                        joinRecord.get(ac.VALUE)
                );

        return dsl.select(ac.asterisk(), ao.OFFICE_ID).from(
                ac.join(ao).on(ac.OFFICE_CODE.eq(ao.OFFICE_CODE))).where(cond).fetch(mapper);
    }

    public String getClobValue(String office, String id) {
        AV_CLOB ac = AV_CLOB.AV_CLOB;
        AV_OFFICE ao = AV_OFFICE.AV_OFFICE;

        Condition cond = ac.ID.eq(id).and(ao.OFFICE_ID.eq(office));

        Record1<String> clobRecord = dsl.select(ac.VALUE).from(
                ac.join(ao).on(ac.OFFICE_CODE.eq(ao.OFFICE_CODE))).where(cond).fetchOne();

        return clobRecord.value1();
    }

    public void create(Clob clob, boolean failIfExists) {

        String pFailIfExists = getBoolean(failIfExists);
        dsl.connection(c -> CWMS_TEXT_PACKAGE.call_STORE_TEXT(
            getDslContext(c, clob.getOfficeId()).configuration(),
            clob.getValue(),
            clob.getId(),
            clob.getDescription(),
            pFailIfExists,
            clob.getOfficeId()));
    }

    @NotNull
    public static String getBoolean(boolean failIfExists) {
        String pFailIfExists;
        if (failIfExists) {
            pFailIfExists = "T";
        } else {
            pFailIfExists = "F";
        }
        return pFailIfExists;
    }

    public void delete(String officeId, String id) {
        dsl.connection(c -> CWMS_TEXT_PACKAGE.call_DELETE_TEXT(
                getDslContext(c,officeId).configuration(), id, officeId)
        );
    }

    public void update(Clob clob, boolean ignoreNulls) {

        String p_ignore_nulls = getBoolean(ignoreNulls);

        // Note: when p_ignore_nulls == 'T' and the value or description is "" (not null)
        // the field is not updated.
        // Also note: when p_ignore_nulls == 'F' and the value is null
        // it throws -  ORA-20244: NULL_ARGUMENT: Argument P_TEXT is not allowed to be null
        // Also note: when p_ignore_nulls == 'F' and the value is "" (empty string)
        // it throws -  ORA-20244: NULL_ARGUMENT: Argument P_TEXT is not allowed to be null
        dsl.connection(c ->
            CWMS_TEXT_PACKAGE.call_UPDATE_TEXT(
                getDslContext(c,clob.getOfficeId()).configuration(),
                clob.getValue(),
                clob.getId(),
                clob.getDescription(),
                p_ignore_nulls,
                clob.getOfficeId()
            )
        );
    }

    /**
     *
     * @param clobId the id to search for
     * @param officeId the office
     * @param clobConsumer a consumer that should be handed the input stream and the length of the stream.
     */
    public void getClob(String clobId, String officeId, ClobConsumer clobConsumer) {
        // Not using jOOQ here because we want the java.sql.Clob and not an automatic field binding.  We want
        // clob so that we can pull out a stream to the data and pass that to javalin.
        // If the request included Content-Ranges Javalin can have the stream skip to the correct
        // location, which will avoid reading unneeded data.  Passing this stream right to the javalin
        // response should let CDA return a huge (2Gb) clob to the client without ever holding the entire String
        // in memory.
        // We can't use the stream once the connection we get from jooq is closed, so we have to pass in
        // what we want javalin to do with the stream as a consumer.
        //

        dsl.connection(connection -> {
            try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_CLOB_QUERY)) {
                preparedStatement.setString(1, officeId);
                preparedStatement.setString(2, clobId);

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        java.sql.Clob clob = resultSet.getClob("VALUE");
                        if (clob != null) {
                            clobConsumer.accept(clob);
                        } else {
                            clobConsumer.accept(null);
                        }
                    } else {
                        throw new NotFoundException("Unable to find clob with id " + clobId + " in office " + officeId);
                    }
                }
            }
        });
    }

    public static String readFully(java.sql.Clob clob) throws IOException, SQLException {
        try (Reader reader = clob.getCharacterStream();
            BufferedReader br = new BufferedReader(reader)) {
            StringBuilder sb = new StringBuilder();
            String line;
            while (null != (line = br.readLine())) {
                sb.append(line);
            }
            return sb.toString();
        }
    }

    @FunctionalInterface
    public interface ClobConsumer {
        void accept(java.sql.Clob blob) throws SQLException;
    }
}
