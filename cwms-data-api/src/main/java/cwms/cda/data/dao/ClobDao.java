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
            + "where av_office.office_id = ? and upper(av_clob.id) = upper(?)";

    public ClobDao(DSLContext dsl) {
        super(dsl);
    }

    @Override
    public Optional<Clob> getByUniqueName(String uniqueName, String office) {
        AV_CLOB vClob = AV_CLOB.AV_CLOB;
        AV_OFFICE vOffice = AV_OFFICE.AV_OFFICE;

        Condition cond = upper(vClob.ID).eq(upper(uniqueName));
        if (office != null && !office.isEmpty()) {
            cond = cond.and(vOffice.OFFICE_ID.eq(office));
        }

        RecordMapper<Record, Clob> mapper = joinRecord ->
                new Clob(joinRecord.getValue(vOffice.OFFICE_ID),
                        joinRecord.getValue(vClob.ID),
                        joinRecord.getValue(vClob.DESCRIPTION),
                        joinRecord.getValue(vClob.VALUE)
                );

        return dsl.select(vOffice.OFFICE_ID, vClob.asterisk())
                .from(vClob.join(vOffice).on(vClob.OFFICE_CODE.eq(vOffice.OFFICE_CODE)))
                .where(cond)
                .fetchOptional(mapper);
    }

    public Clobs getClobs(String cursor, int pageSize, String officeLike,
                          boolean includeValues, String idRegex) {
        int total = 0;
        String cursorOffice = null;
        String cursorClobId = null;
        AV_CLOB vClob = AV_CLOB.AV_CLOB;
        AV_OFFICE vOffice = AV_OFFICE.AV_OFFICE;

        Condition whereClause = JooqDao.caseInsensitiveLikeRegex(vClob.ID, idRegex)
            .and(JooqDao.caseInsensitiveLikeRegexNullTrue(vOffice.OFFICE_ID, officeLike));
        if (cursor == null || cursor.isEmpty()) {
            SelectConditionStep<Record1<Integer>> count = dsl.select(count(asterisk()))
                .from(vClob)
                .join(vOffice).on(vClob.OFFICE_CODE.eq(vOffice.OFFICE_CODE))
                .where(whereClause);
            Record1<Integer> rec = count.fetchOne();
            if(rec != null) {
                total = rec.value1();
            }
        } else {
            final String[] parts = CwmsDTOPaginated.decodeCursor(cursor, "||");

            logger.atFine().log("decoded cursor: " + String.join("||", parts));
            for (String p : parts) {
                logger.atFinest().log(p);
            }

            if (parts.length > 1) {
                cursorOffice = Clobs.getOffice(cursor);
                cursorClobId = Clobs.getId(cursor);
                total = Integer.parseInt(parts[1]);
                pageSize = Integer.parseInt(parts[2]);
            }
        }

        Condition moreInSameOffice = cursorClobId == null || cursorOffice == null ? noCondition() :
                vOffice.OFFICE_ID.eq(cursorOffice.toUpperCase())
                        .and(upper(vClob.ID).greaterThan(cursorClobId.toUpperCase()));
        Condition nextOffices = cursorOffice == null ? noCondition():
                upper(vOffice.OFFICE_ID).greaterThan(cursorOffice.toUpperCase());
        Condition pagingCondition = moreInSameOffice.or(nextOffices);

        SelectLimitPercentStep<Record4<String, String, String, String>> query = dsl.select(
                vOffice.OFFICE_ID,
                vClob.ID,
                vClob.DESCRIPTION,
                includeValues ? vClob.VALUE : DSL.inline("").as(vClob.VALUE)
            )
            .from(vClob)
            .join(vOffice).on(vClob.OFFICE_CODE.eq(vOffice.OFFICE_CODE))
            .where(whereClause)
            .and(pagingCondition)
            .orderBy(vOffice.OFFICE_ID, vClob.ID)
            .limit(pageSize);


        Clobs.Builder builder = new Clobs.Builder(cursor, pageSize, total);

        logger.atFine().log(query.getSQL(ParamType.INLINED));

        query.fetch().forEach(row -> {
            usace.cwms.db.jooq.codegen.tables.records.AV_CLOB clob = row.into(vClob);
            usace.cwms.db.jooq.codegen.tables.records.AV_OFFICE clobOffice = row.into(vOffice);
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
        AV_CLOB vClob = AV_CLOB.AV_CLOB;
        AV_OFFICE vOffice = AV_OFFICE.AV_OFFICE;

        Condition cond = caseInsensitiveLikeRegex(vClob.ID, idLike);
        if (office != null && !office.isEmpty()) {
            cond = cond.and(DSL.upper(vOffice.OFFICE_ID).eq(office.toUpperCase()));
        }

        RecordMapper<Record, Clob> mapper = joinRecord ->
                new Clob(joinRecord.get(vOffice.OFFICE_ID),
                        joinRecord.get(vClob.ID),
                        joinRecord.get(vClob.DESCRIPTION),
                        joinRecord.get(vClob.VALUE)
                );

        return dsl.select(vClob.asterisk(), vOffice.OFFICE_ID)
                .from(vClob.join(vOffice).on(vClob.OFFICE_CODE.eq(vOffice.OFFICE_CODE)))
                .where(cond)
                .orderBy(vOffice.OFFICE_ID, vClob.ID)
                .fetch(mapper);
    }

    public void create(Clob clob, boolean failIfExists) {

        String pFailIfExists = getBoolean(failIfExists);
        connection(dsl, c ->
            CWMS_TEXT_PACKAGE.call_STORE_TEXT(
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

        String pIgnoreNulls = getBoolean(ignoreNulls);

        // Note: when pIgnoreNulls == 'T' and the value or description is "" (not null)
        // the field is not updated.
        // Also note: when pIgnoreNulls == 'F' and the value is null
        // it throws -  ORA-20244: NULL_ARGUMENT: Argument P_TEXT is not allowed to be null
        // Also note: when pIgnoreNulls == 'F' and the value is "" (empty string)
        // it throws -  ORA-20244: NULL_ARGUMENT: Argument P_TEXT is not allowed to be null
        dsl.connection(c ->
            CWMS_TEXT_PACKAGE.call_UPDATE_TEXT(
                getDslContext(c,clob.getOfficeId()).configuration(),
                clob.getValue(),
                clob.getId(),
                clob.getDescription(),
                pIgnoreNulls,
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
                        clobConsumer.accept(clob);
                    } else {
                        throw new NotFoundException("Unable to find clob with id " + clobId + " in office " + officeId);
                    }
                }
            }
        });
    }

    public static String readFully(java.sql.Clob clob) throws IOException, SQLException {
        try(Reader reader = clob.getCharacterStream();
            BufferedReader br = new BufferedReader(reader)) {
            StringBuilder sb = new StringBuilder();
            String line;
            while(null != (line = br.readLine())) {
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
