package cwms.cda.data.dao.binarytimeseries;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.binarytimeseries.BinaryTimeSeries;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.RecordMapper;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_TEXT_PACKAGE;

public class TimeSeriesBinaryDao extends JooqDao<BinaryTimeSeries> {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();


    public static final String OFFICE_ID = "OFFICE_ID";


    public TimeSeriesBinaryDao(DSLContext dsl) {
        super(dsl);
    }

//    @Nullable
//    public static Date getDate(@Nullable Instant startTime) {
//        Date startDate;
//        if (startTime != null) {
//            startDate = Date.from(startTime);
//        } else {
//            startDate = null;
//        }
//        return startDate;
//    }
//
//    @Nullable
//    public static Date getDate(@Nullable Timestamp startTime) {
//        Date startDate;
//        if (startTime != null) {
//            startDate = new Date(startTime.getTime());
//        } else {
//            startDate = null;
//        }
//        return startDate;
//    }
//
//    @Nullable
//    private Timestamp getTimestamp(Instant startTime) {
//        Timestamp retval = null;
//
//        if(startTime != null){
//            retval = Timestamp.from(startTime);
//        }
//
//        return retval;
//    }

    public void delete(String officeId, String tsId, String binaryTypeMask,
                       ZonedDateTime startTime, ZonedDateTime endTime, ZonedDateTime versionInstant, boolean maxVersion,
                       Long minAttribute, Long maxAttribute){
        delete(officeId, tsId, binaryTypeMask,
                startTime == null ? null : startTime.toInstant(),
                endTime == null ? null : endTime.toInstant(),
                versionInstant == null ? null : versionInstant.toInstant(),
                maxVersion, minAttribute, maxAttribute);
    }

    private void delete(String officeId, String tsId, String binaryTypeMask,
                       Instant startTime, Instant endTime, Instant versionInstant, boolean maxVersion,
                       Long minAttribute, Long maxAttribute) {
        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;

        CWMS_TEXT_PACKAGE.call_DELETE_TS_BINARY(
                dsl.configuration(),
                tsId, binaryTypeMask,
                startTime == null ? null : Timestamp.from(startTime),
                endTime == null ? null : Timestamp.from(endTime),
                versionInstant == null ? null : Timestamp.from(versionInstant),
                timeZone.getID(),
                maxVersion ? "T" : "F", minAttribute, maxAttribute, officeId);

    }


    /**
     *
     * @param officeId
     * @param tsId
     * @param binaryData
     * @param binaryType
     * @param startTime
     * @param endTime
     * @param versionInstant
     * @param maxVersion  default is T
     * @param storeExisting  default is T
     * @param storeNonExisting default is F
     * @param replaceAll default is F
     * @param attribute
     */
    public void store(String officeId, String tsId, byte[] binaryData, String binaryType, Instant startTime, Instant endTime, Instant versionInstant,
                      boolean maxVersion, boolean storeExisting, boolean storeNonExisting, boolean replaceAll, Long attribute) {

        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;


        CWMS_TEXT_PACKAGE.call_STORE_TS_BINARY(
                dsl.configuration(),
                tsId,
                binaryData,
                binaryType,
                startTime == null ? null : Timestamp.from(startTime),
                endTime == null ? null : Timestamp.from(endTime),
                versionInstant == null ? null : Timestamp.from(versionInstant),
                timeZone.getID(),
                maxVersion? "T":"F", storeExisting?"T":"F", storeNonExisting? "T":"F", replaceAll? "T":"F", attribute, officeId) ;
    }

    public static class BinaryRecord {
        private final Date dateTime;
        private final Date versionDate;

        private final Date dataEntryDate;
        private final String binaryId;
        private final Long attribute;
        private final String mediaType;
        private final String fileExtension;
        private final byte[] binaryData;


        public BinaryRecord(Date dateTime, Date versionDate, Date dataEntryDate, String binaryId, Long attribute, String mediaType, String fileExtension, byte[] binaryData) {
            this.dateTime = dateTime;
            this.versionDate = versionDate;
            this.dataEntryDate = dataEntryDate;
            this.binaryId = binaryId;
            this.attribute = attribute;
            this.mediaType = mediaType;
            this.fileExtension = fileExtension;
            this.binaryData = binaryData;
        }

        public Date getDateTime() {
            return dateTime;
        }

        public Date getVersionDate() {
            return versionDate;
        }

        public Date getDataEntryDate() {
            return dataEntryDate;
        }

        public String getBinaryId() {
            return binaryId;
        }

        public Long getAttribute() {
            return attribute;
        }

        public String getMediaType() {
            return mediaType;
        }

        public String getFileExtension() {
            return fileExtension;
        }

        public byte[] getBinaryData() {
            return binaryData;
        }
    }

    public List<BinaryRecord> retrieve(String officeId, String tsId, String mask,
                                       Instant startTime, Instant endTime, Instant versionInstant,
                                       boolean maxVersion, boolean retrieveBinary, Long minAttribute, Long maxAttribute) {

        TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;


        RecordMapper<? super Record, BinaryRecord> mapper = new RecordMapper<Record, BinaryRecord>() {
            @Override
            public @Nullable BinaryRecord map(Record record) {
                for (Field<?> field : record.fields()) {
                    logger.atInfo().log("Field: %s name:%s", field, field.getName());
                }

                Date dateTime = record.get("DATE_TIME", Date.class);
                Date versionDate = record.get("VERSION_DATE", Date.class);
                Timestamp dataEntryDate = record.get("DATA_ENTRY_DATE", Timestamp.class);
                String binaryId = record.get("ID", String.class);
                BigDecimal attribute = record.get("ATTRIBUTE", BigDecimal.class);
                String mediaType = record.get("MEDIA_TYPE_ID", String.class);
                String fileExtension = record.get("FILE_EXT", String.class);
                byte[] binaryData = record.get("VALUE", byte[].class);
                return new BinaryRecord(dateTime, versionDate, dataEntryDate, binaryId, attribute.longValueExact(), mediaType, fileExtension, binaryData);
            }
        };

        return CWMS_TEXT_PACKAGE.call_RETRIEVE_TS_BINARY(dsl.configuration(),
                        tsId, mask,
                        startTime == null ? null : Timestamp.from(startTime),
                        endTime == null ? null : Timestamp.from(endTime),
                        versionInstant == null ? null : Timestamp.from(versionInstant),
                        timeZone.getID(),
                        maxVersion?"T": "F", retrieveBinary?"T": "F", minAttribute, maxAttribute, officeId)
                .map(mapper);

    }

}
