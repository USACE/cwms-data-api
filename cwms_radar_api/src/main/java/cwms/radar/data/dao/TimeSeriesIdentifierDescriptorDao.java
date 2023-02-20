package cwms.radar.data.dao;

import static usace.cwms.db.dao.util.OracleTypeMap.parseBool;
import static usace.cwms.db.dao.util.OracleTypeMap.toZoneId;

import cwms.radar.data.dto.TimeSeriesIdentifierDescriptor;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.jooq.Condition;
import org.jooq.DSLContext;
import usace.cwms.db.dao.ifc.ts.CwmsDbTs;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID2;

public class TimeSeriesIdentifierDescriptorDao extends JooqDao<TimeSeriesIdentifierDescriptor> {


    public TimeSeriesIdentifierDescriptorDao(DSLContext dsl) {
        super(dsl);
    }


    public List<TimeSeriesIdentifierDescriptor> getTimeSeriesIdentifiers(String office, String idRegex) {

        Condition whereCondition;
        if (idRegex == null || idRegex.isEmpty()) {
            whereCondition = AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.DB_OFFICE_ID.equalIgnoreCase(office);
        } else {
            whereCondition = AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.DB_OFFICE_ID.equalIgnoreCase(office)
                    .and(JooqDao.caseInsensitiveLikeRegex(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.CWMS_TS_ID, idRegex));
        }

        List<TimeSeriesIdentifierDescriptor> retval = dsl
                .select(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.DB_OFFICE_ID,
                        AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.CWMS_TS_ID,
                        AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.INTERVAL_UTC_OFFSET,
                        AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.TS_ACTIVE_FLAG,
                        AV_CWMS_TS_ID2.AV_CWMS_TS_ID2.TIME_ZONE_ID)
                .from(AV_CWMS_TS_ID2.AV_CWMS_TS_ID2)
                .where(whereCondition)
                .stream()
                .map(r -> toDescriptor( r))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return retval;
    }

    private TimeSeriesIdentifierDescriptor toDescriptor(org.jooq.Record5<String, String, BigDecimal, String, String> r)
    {
        String officeId = r.get(r.field1());
        String tsId = r.get(r.field2());
        BigDecimal utcOffset = r.get(r.field3());
        String activeFlag = r.get(r.field4());
        String zoneId = r.get(r.field5());

        String locationId = tsId.substring(0, tsId.indexOf('.'));


        return new TimeSeriesIdentifierDescriptor.Builder()
                .withOfficeId(officeId)
                .withTimeSeriesId(tsId)
                .withZoneId(toZoneId(zoneId, locationId))
                .withIntervalOffsetMinutes(utcOffset.longValueExact())
                .withActive(parseBool(activeFlag))
                .build();
    }


    public Optional<TimeSeriesIdentifierDescriptor> getTimeSeriesIdentifier(String office, String timeseriesId) {
        Optional<TimeSeriesIdentifierDescriptor> retval;
        return connectionResult(dsl, connection -> {
            CwmsDbTs tsDao = CwmsDbServiceLookup.buildCwmsDb(CwmsDbTs.class, connection);
            Optional<usace.cwms.db.dao.ifc.ts.TimeSeriesIdentifierDescriptor> tsIdDesc = tsDao.retrieveTSIdentifierWithAliasSupport(connection, office, timeseriesId);

            return toDTO(tsIdDesc);
        });
    }

    public static Optional<TimeSeriesIdentifierDescriptor> toDTO(Optional<usace.cwms.db.dao.ifc.ts.TimeSeriesIdentifierDescriptor> tsIdDesc) {
        Optional<TimeSeriesIdentifierDescriptor> retval = Optional.empty();
        if (tsIdDesc.isPresent()) {
            retval = Optional.of(toDTO(tsIdDesc.get()));
        }

        return retval;
    }

    public static TimeSeriesIdentifierDescriptor toDTO(usace.cwms.db.dao.ifc.ts.TimeSeriesIdentifierDescriptor tsId) {
        TimeSeriesIdentifierDescriptor dto = new TimeSeriesIdentifierDescriptor.Builder()
                .withOfficeId(tsId.getOfficeId())
                .withTimeSeriesId(tsId.getTimeSeriesId())
                .withZoneId(tsId.getZoneId())
                .withIntervalOffsetMinutes(tsId.getIntervalOffsetMinutes())
                .withActive(tsId.isActive())
                .build();
        return dto;
    }

    public void update(String office, String timeseriesId, Number utcOffsetMinutes, Number intervalForward,
                       Number intervalBackward, boolean activeFlag) {

        connection(dsl, connection -> {
            CwmsDbTs tsDao = CwmsDbServiceLookup.buildCwmsDb(CwmsDbTs.class, connection);
            tsDao.updateTsId(connection, office, timeseriesId, utcOffsetMinutes, intervalForward, intervalBackward, activeFlag);
        });

    }

    public void rename (String officeId, String origId, String newId, Long utcOffset){

        if(utcOffset == null){
            CWMS_TS_PACKAGE.call_RENAME_TS(dsl.configuration(), officeId, origId, newId);
        } else {
            CWMS_TS_PACKAGE.call_RENAME_TS__2(dsl.configuration(), origId, newId, utcOffset, officeId);
        }
    }

    public void deleteAll(String officeId, String tsId){

        connection(dsl, connection -> {
            CwmsDbTs tsDao = CwmsDbServiceLookup.buildCwmsDb(CwmsDbTs.class, connection);
            tsDao.deleteAll(connection, officeId, tsId);

            // If we decide to call the proc directory I think the choices are:
            // cwms_util.delete_key, cwms_util.delete_data, cwms_util.delete_all,
            // cwms_util.delete_ts_id, cwms_util.delete_ts_data, cwms_util.delete_ts_cascade
        });
    }

    public void delete(String officeId, String tsId, DeleteOptions options){



        connection(dsl, connection -> {
            CwmsDbTs tsDao = CwmsDbServiceLookup.buildCwmsDb(CwmsDbTs.class, connection);
            tsDao.deleteTs(connection, officeId, tsId, options.getStartTime(), options.getEndTime(),
                    options.isStartTimeInclusive(), options.isEndTimeInclusive(),
                    options.getVersionDate(), null, options.getMaxVersion(),
                    options.getTsItemMask(), options.getOverrideProtection());
        });
    }



    public enum OverrideProtection
    {
        /**
         * If set to True, all specified values are quietly deleted
         */
        True,
        /**
         * If set to False, only non-protected values are quietly
         * deleted.
         */
        False,
        /**
         * If set to E, all specified values are deleted only if
         * all values are non-protected values. If protected values are present,
         * then no values are deleted and the following error is raised:
         * cwms_err.raise('ERROR', 'One or more values are protected').
         */
        E;

        @Override
        public String toString()
        {
            return name().substring(0, 1);
        }
    }

    public static class DeleteOptions{
        private Date startTime;
        private Date endTime;
        private boolean startTimeInclusive;
        private boolean endTimeInclusive;
        private Date versionDate;

        // dateTimeSet isn't used, near as I can tell.
//        private NavigableSet< Date > dateTimesSet;
        private Boolean maxVersion;
        private Integer tsItemMask;
        private String overrideProtection;

        public DeleteOptions(Builder builder) {
            this.startTime = builder.startTime;
            this.endTime = builder.endTime;
            this.startTimeInclusive = builder.startTimeInclusive;
            this.endTimeInclusive = builder.endTimeInclusive;
            this.versionDate = builder.versionDate;
//            this.dateTimesSet = builder.dateTimesSet;
            this.maxVersion = builder.maxVersion;
            this.tsItemMask = builder.tsItemMask;
            this.overrideProtection = builder.overrideProtection;
        }

        public Date getStartTime() {
            return startTime;
        }

        public Date getEndTime() {
            return endTime;
        }

        public boolean isStartTimeInclusive() {
            return startTimeInclusive;
        }

        public boolean isEndTimeInclusive() {
            return endTimeInclusive;
        }

        public Date getVersionDate() {
            return versionDate;
        }

//        public NavigableSet<Date> getDateTimesSet() {
//            return dateTimesSet;
//        }

        public Boolean getMaxVersion() {
            return maxVersion;
        }

        public Integer getTsItemMask() {
            return tsItemMask;
        }

        public String getOverrideProtection() {
            return overrideProtection;
        }

        public static class Builder {
            private Date startTime;
            private Date endTime;
            private boolean startTimeInclusive = true;
            private boolean endTimeInclusive = true;
            private Date versionDate;
            private NavigableSet< Date > dateTimesSet;
            private Boolean maxVersion = null;
            private Integer tsItemMask = null;
            private String overrideProtection;

            public Builder withStartTime(Date startTime){
                this.startTime = startTime;
                return this;
            }

            public Builder withEndTime(Date endTime){
                this.endTime = endTime;
                return this;
            }

            public Builder withStartTimeInclusive(boolean startTimeInclusive){
                this.startTimeInclusive = startTimeInclusive;
                return this;
            }

            public Builder withEndTimeInclusive(boolean endTimeInclusive){
                this.endTimeInclusive = endTimeInclusive;
                return this;
            }

            public Builder withVersionDate(Date versionDate){
                this.versionDate = versionDate;
                return this;
            }

//            public Builder withDateTimesSet(NavigableSet< Date > dateTimesSet){
//                this.dateTimesSet = dateTimesSet;
//                return this;
//            }

            public Builder withMaxVersion(Boolean maxVersion){
                this.maxVersion = maxVersion;
                return this;
            }

            public Builder withTsItemMask(Integer tsItemMask){
                this.tsItemMask = tsItemMask;
                return this;
            }

            public Builder withOverrideProtection(String overrideProtection){
                this.overrideProtection = overrideProtection;
                return this;
            }

            public DeleteOptions build(){
                return new DeleteOptions(this);
            }


        }
    }

}
