package cwms.cda.data.dao.timeseriesprofile;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static usace.cwms.db.jooq.codegen.tables.AV_TS_PROFILE.AV_TS_PROFILE;

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.jooq.SelectSeekStep2;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PROFILE_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.STR_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.TS_PROFILE_T;


public class TimeSeriesProfileDao extends JooqDao<TimeSeriesProfile> {
    public TimeSeriesProfileDao(DSLContext dsl) {
        super(dsl);
    }

    public void storeTimeSeriesProfile(TimeSeriesProfile timeSeriesProfile, boolean failIfExists) {

        connection(dsl, conn -> {
            setOffice(conn, timeSeriesProfile.getLocationId().getOfficeId());
            List<String> parameterList = timeSeriesProfile.getParameterList();
            StringBuilder parameterString = new StringBuilder(parameterList.get(0));

            for (int i = 1; i < parameterList.size(); i++) {
                parameterString.append(",").append(parameterList.get(i));
            }
            String referenceTsId = null;
            if (timeSeriesProfile.getReferenceTsId() != null) {
                referenceTsId = timeSeriesProfile.getReferenceTsId().getName();
            }
            setOffice(conn, timeSeriesProfile.getLocationId().getOfficeId());
            CWMS_TS_PROFILE_PACKAGE.call_STORE_TS_PROFILE(DSL.using(conn).configuration(),
                    timeSeriesProfile.getLocationId().getName(),
                    timeSeriesProfile.getKeyParameter(),
                    parameterString.toString(),
                    timeSeriesProfile.getDescription(), referenceTsId, failIfExists ? "T" : "F", "T",
                    timeSeriesProfile.getLocationId().getOfficeId());
        });
    }

    public TimeSeriesProfile retrieveTimeSeriesProfile(String locationId, String parameterId, String officeId) {
        return connectionResult(dsl, conn -> {
            setOffice(conn, officeId);
            TS_PROFILE_T timeSeriesProfile = CWMS_TS_PROFILE_PACKAGE.call_RETRIEVE_TS_PROFILE(
                    DSL.using(conn).configuration(), locationId, parameterId, officeId);
            return map(timeSeriesProfile, locationId, parameterId, officeId);
        });
    }

    public void deleteTimeSeriesProfile(String locationId, String keyParameter, String officeId) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_TS_PROFILE_PACKAGE.call_DELETE_TS_PROFILE(DSL.using(conn).configuration(), locationId, keyParameter,
                    "DELETE ALL", officeId);
        });
    }

    public void copyTimeSeriesProfile(String locationId, String keyParameter, String destinationLocation,
            String destRefTsId, String officeId) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            CWMS_TS_PROFILE_PACKAGE.call_COPY_TS_PROFILE(DSL.using(conn).configuration(), locationId, keyParameter,
                    destinationLocation, destRefTsId, "F", "F", officeId);
        });
    }

    public TimeSeriesProfileList catalogTimeSeriesProfiles(String locationIdMask, String parameterIdMask,
             String officeIdMask, String page, int pageSize) {
        String cursor = null;
        Long tsCursor = null;
        String parameterId = null;
        Integer total = null;

        // Decode the cursor
        if (page != null && !page.isEmpty()) {
            final String[] parts = CwmsDTOPaginated.decodeCursor(page);

            if (parts.length > 1) {
                cursor = parts[0];
                tsCursor = Long.parseLong(parts[0]);

                if (parts.length > 2) {
                    parameterId = parts[1];
                    total = Integer.parseInt(parts[2]);
                }
            }
        }

        // Initialize the where condition
        Condition whereCondition = JooqDao.caseInsensitiveLikeRegexNullTrue(AV_TS_PROFILE.LOCATION_ID, locationIdMask);
        whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(AV_TS_PROFILE.OFFICE_ID, officeIdMask));
        whereCondition = whereCondition.and(JooqDao.caseInsensitiveLikeRegex(AV_TS_PROFILE.KEY_PARAMETER_ID,
                parameterIdMask));

        if (tsCursor != null) {
            whereCondition = whereCondition.and(AV_TS_PROFILE.LOCATION_CODE.greaterThan(tsCursor));
        }

        // Get the total count if not already set
        if (total == null) {
            SelectConditionStep<Record1<Integer>> count = dsl.select(count(asterisk()))
                .from(AV_TS_PROFILE)
                .where(whereCondition);
            total = count.fetchOne().value1();
        }

        // initialize the return object
        TimeSeriesProfileList timeSeriesProfileList = new TimeSeriesProfileList.Builder()
            .page(page)
            .pageSize(pageSize)
            .total(total)
            .timeSeriesProfileList(new ArrayList<>())
            .build();

        // Get the time series profiles
        @NotNull Result<Record> timeSeriesProfileResults;
        SelectSeekStep2<Record, Long, String> selectionStep = dsl.select(DSL.asterisk()).from(AV_TS_PROFILE)
            .where(whereCondition)
            .orderBy(AV_TS_PROFILE.LOCATION_CODE, AV_TS_PROFILE.KEY_PARAMETER_ID);

        // Use the cursor if it is set
        // Start page at the cursor using JOOQ seek method
        if (pageSize > 0) {
            if (cursor != null) {
                timeSeriesProfileResults = selectionStep.seek(tsCursor, parameterId).limit(pageSize + 1).fetch();
            } else {
                timeSeriesProfileResults = selectionStep.limit(pageSize + 1).fetch();
            }
        } else {
            timeSeriesProfileResults = selectionStep.fetch();
        }

        // If there are no results, return the empty list
        if (timeSeriesProfileResults.isEmpty()) {
            return timeSeriesProfileList;
        }

        // Map the results to the return object
        // Initialize the previous location code and parameter id to split the pages correctly
        Long prevLocationCode = timeSeriesProfileResults.get(0).get("LOCATION_CODE", Long.class);
        String prevParameterId = timeSeriesProfileResults.get(0).get("KEY_PARAMETER_ID", String.class);
        for (Record timeSeriesProfileResult : timeSeriesProfileResults) {
            String parameters = timeSeriesProfileResult.get("PARAMETERS", String.class);
            String[] parameterArray = parameters.split(",");
            List<String> parameterList = Arrays.asList(parameterArray);

            CwmsId locationId = new CwmsId.Builder()
                .withName((String) timeSeriesProfileResult.get("LOCATION_ID"))
                .withOfficeId((String) timeSeriesProfileResult.get("OFFICE_ID"))
                .build();
            CwmsId referenceTsId = new CwmsId.Builder()
                .withName((String) timeSeriesProfileResult.get("ELEV_TS_ID"))
                .withOfficeId((String) timeSeriesProfileResult.get("OFFICE_ID"))
                .build();

            // Add the value to the return object
            // Adds page breaks if the page size is reached
            timeSeriesProfileList.addValue((Long) timeSeriesProfileResult.get("LOCATION_CODE"),
                new TimeSeriesProfile.Builder().withDescription((String) timeSeriesProfileResult.get("DESCRIPTION"))
                    .withReferenceTsId(referenceTsId)
                    .withKeyParameter((String) timeSeriesProfileResult.get("KEY_PARAMETER_ID"))
                    .withLocationId(locationId)
                    .withParameterList(parameterList)
                    .build(),
                prevParameterId, prevLocationCode)
            ;
            prevLocationCode = (Long) timeSeriesProfileResult.get("LOCATION_CODE");
            prevParameterId = (String) timeSeriesProfileResult.get("KEY_PARAMETER_ID");
        }
        return timeSeriesProfileList;
    }

    private TimeSeriesProfile map(TS_PROFILE_T timeSeriesProfile, String locationName, String keyParameter,
            String officeId) {
        STR_TAB_T profileParams = timeSeriesProfile.getPROFILE_PARAMS();
        List<String> parameterList = new ArrayList<>(profileParams);
        CwmsId locationId = new CwmsId.Builder().withName(locationName).withOfficeId(officeId).build();
        CwmsId referenceTsId = null;
        if (timeSeriesProfile.getREFERENCE_TS_ID() != null) {
            referenceTsId = new CwmsId.Builder().withName(timeSeriesProfile.getREFERENCE_TS_ID())
                    .withOfficeId(officeId).build();
        }
        return new TimeSeriesProfile.Builder()
                .withLocationId(locationId)
                .withDescription(timeSeriesProfile.getDESCRIPTION())
                .withReferenceTsId(referenceTsId)
                .withKeyParameter(keyParameter)
                .withParameterList(parameterList)
                .build();
    }
}
