package cwms.cda.data.dao;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.noCondition;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.Project;
import cwms.cda.data.dto.Projects;
import cwms.cda.helpers.ResourceHelper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.logging.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.jooq.SelectConditionStep;
import org.jooq.SelectLimitPercentStep;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.ifc.loc.LocationRefType;
import usace.cwms.db.dao.ifc.loc.LocationType;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_PROJECT;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_OBJ_T;
import usace.cwms.db.jooq.dao.util.LocationTypeUtil;

public class ProjectDao extends JooqDao<Project> {
    private static final Logger logger = Logger.getLogger(ProjectDao.class.getName());
    public static final String OFFICE_ID = "office_id";
    public static final String PROJECT_ID = "project_id";
    public static final String AUTHORIZING_LAW = "authorizing_law";
    public static final String PROJECT_OWNER = "project_owner";
    public static final String HYDROPOWER_DESCRIPTION = "hydropower_description";
    public static final String SEDIMENTATION_DESCRIPTION = "sedimentation_description";
    public static final String DOWNSTREAM_URBAN_DESCRIPTION = "downstream_urban_description";
    public static final String BANK_FULL_CAPACITY_DESCRIPTION = "bank_full_capacity_description";
    public static final String PUMP_BACK_OFFICE_ID = "pump_back_office_id";
    public static final String PUMP_BACK_LOCATION_ID = "pump_back_location_id";
    public static final String NEAR_GAGE_OFFICE_ID = "near_gage_office_id";
    public static final String NEAR_GAGE_LOCATION_ID = "near_gage_location_id";
    public static final String PROJECT_REMARKS = "project_remarks";
    public static final String FEDERAL_COST = "federal_cost";
    public static final String NONFEDERAL_COST = "nonfederal_cost";
    public static final String FEDERAL_OM_COST = "FEDERAL_OM_COST";
    public static final String NONFEDERAL_OM_COST = "NONFEDERAL_OM_COST";
    public static final String COST_YEAR = "COST_YEAR";
    public static final String YIELD_TIME_FRAME_START = "yield_time_frame_start";
    public static final String YIELD_TIME_FRAME_END = "yield_time_frame_end";

    private final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    private static final String SELECT_PART =
            ResourceHelper.getResourceAsString("/cwms/data/sql/project/project_select.sql", ProjectDao.class);


    public ProjectDao(DSLContext dsl) {
        super(dsl);
    }

    public Project retrieveProject(String office, String projectId) {

        PROJECT_OBJ_T projectObjT = connectionResult(dsl,
                c -> CWMS_PROJECT_PACKAGE.call_RETRIEVE_PROJECT(
                        getDslContext(c, office).configuration(), projectId, office)
        );

        return projectObjT == null ? null : buildProject(projectObjT);
    }

    private Project buildProject(PROJECT_OBJ_T projectObjT) {
        Project.Builder builder = new Project.Builder();

        LOCATION_OBJ_T projectLocation = projectObjT.getPROJECT_LOCATION();
        LOCATION_REF_T locRef = projectLocation.getLOCATION_REF();
        String office = locRef.getOFFICE_ID();
        builder.withOfficeId(office);

        String id = getLocationId(locRef.getBASE_LOCATION_ID(), locRef.getSUB_LOCATION_ID());
        builder.withName(id);

        String authorizingLaw = projectObjT.getAUTHORIZING_LAW();
        builder.withAuthorizingLaw(authorizingLaw);

        String bankFullCapacityDescription = projectObjT.getBANK_FULL_CAPACITY_DESCRIPTION();
        builder.withBankFullCapacityDesc(bankFullCapacityDescription);

        String downstreamUrbanDescription = projectObjT.getDOWNSTREAM_URBAN_DESCRIPTION();
        builder.withDownstreamUrbanDesc(downstreamUrbanDescription);

        String costUnitsId = projectObjT.getCOST_UNITS_ID();
        builder.withCostUnit(costUnitsId);

        String projectOwner = projectObjT.getPROJECT_OWNER();
        builder.withProjectOwner(projectOwner);

        String hydropowerDescription = projectObjT.getHYDROPOWER_DESCRIPTION();
        builder.withHydropowerDesc(hydropowerDescription);

        String remarks = projectObjT.getREMARKS();
        builder.withProjectRemarks(remarks);

        String sedimentationDescription = projectObjT.getSEDIMENTATION_DESCRIPTION();
        builder.withSedimentationDesc(sedimentationDescription);

        LocationType neargageLocationType =
                LocationTypeUtil.toLocationType(projectObjT.getNEAR_GAGE_LOCATION());
        LocationRefType nearLocRef = null;
        if (neargageLocationType != null) {
            nearLocRef = neargageLocationType.getLocationRef();
        }
        if (nearLocRef != null) {
            builder = builder.withNearGageLocation(new Location.Builder(nearLocRef.getOfficeId(),
                    getLocationId(nearLocRef.getBaseLocationId(),
                            nearLocRef.getSubLocationId()))
                    .withActive(null)
                    .build()
            );
        }

        LocationType pumpbackLocationType =
                LocationTypeUtil.toLocationType(projectObjT.getPUMP_BACK_LOCATION());
        LocationRefType pumpbackLocRef = null;
        if (pumpbackLocationType != null) {
            pumpbackLocRef = pumpbackLocationType.getLocationRef();
        }
        if (pumpbackLocRef != null) {
            builder = builder.withPumpBackLocation(new Location.Builder(pumpbackLocRef.getOfficeId(),
                    getLocationId(pumpbackLocRef.getBaseLocationId(),
                            pumpbackLocRef.getSubLocationId()))
                    .withActive(null)
                    .build()
            );
        }

        BigDecimal federalCost = projectObjT.getFEDERAL_COST();
        if (federalCost != null) {
            builder = builder.withFederalCost(federalCost.doubleValue());
        }

        BigDecimal federalOandMCost = projectObjT.getFEDERAL_OM_COST();
        if (federalOandMCost != null) {
            builder = builder.withFederalOAndMCost(federalOandMCost.doubleValue());
        }

        BigDecimal nonFederalCost = projectObjT.getNONFEDERAL_COST();
        if (nonFederalCost != null) {
            builder = builder.withNonFederalCost(nonFederalCost.doubleValue());
        }

        BigDecimal nonFederalOandMCost = projectObjT.getNONFEDERAL_OM_COST();
        if (nonFederalOandMCost != null) {
            builder = builder.withNonFederalOAndMCost(nonFederalOandMCost.doubleValue());
        }

        Timestamp costYear = projectObjT.getCOST_YEAR();
        if (costYear != null) {
            builder = builder.withCostYear(costYear.toInstant());
        }

        Timestamp yieldTimeFrameEnd = projectObjT.getYIELD_TIME_FRAME_END();
        if (yieldTimeFrameEnd != null) {
            builder = builder.withYieldTimeFrameEnd(yieldTimeFrameEnd.toInstant());
        }

        Timestamp yieldTimeFrameStart = projectObjT.getYIELD_TIME_FRAME_START();
        if (yieldTimeFrameStart != null) {
            builder = builder.withYieldTimeFrameStart(yieldTimeFrameStart.toInstant());
        }

        return builder.build();
    }



    private static Project buildProject(usace.cwms.db.jooq.codegen.tables.records.AV_PROJECT r) {
        Project.Builder builder = new Project.Builder();
        builder.withOfficeId(r.getOFFICE_ID());
        builder.withName(r.getPROJECT_ID());
        builder.withPumpBackLocation(new Location.Builder(r.getOFFICE_ID(), r.getPUMP_BACK_LOCATION_ID())
                .withActive(null)
                .build()
        ); // Can we assume same office?
        builder.withNearGageLocation(new Location.Builder(r.getOFFICE_ID(), r.getNEAR_GAGE_LOCATION_ID())
                .withActive(null)
                .build()
        ); // Can we assume same office?

        builder.withAuthorizingLaw(r.getAUTHORIZING_LAW());
        builder.withProjectRemarks(r.getPROJECT_REMARKS());
        builder.withProjectOwner(r.getPROJECT_OWNER());
        builder.withHydropowerDesc(r.getHYDROPOWER_DESCRIPTION());
        builder.withSedimentationDesc(r.getSEDIMENTATION_DESCRIPTION());
        builder.withDownstreamUrbanDesc(r.getDOWNSTREAM_URBAN_DESCRIPTION());
        builder.withBankFullCapacityDesc(r.getBANK_FULL_CAPACITY_DESCRIPTION());
        BigDecimal federalCost = r.getFEDERAL_COST();
        if (federalCost != null) {
            builder.withFederalCost(federalCost.doubleValue());
        }
        BigDecimal nonfederalCost = r.getNONFEDERAL_COST();
        if (nonfederalCost != null) {
            builder.withNonFederalCost(nonfederalCost.doubleValue());
        }
        Timestamp yieldTimeFrameStart = r.getYIELD_TIME_FRAME_START();
        if (yieldTimeFrameStart != null) {
            builder.withYieldTimeFrameStart(yieldTimeFrameStart.toInstant());
        }
        Timestamp yieldTimeFrameEnd = r.getYIELD_TIME_FRAME_END();
        if (yieldTimeFrameEnd != null) {
            builder.withYieldTimeFrameEnd(yieldTimeFrameEnd.toInstant());
        }

        // The view is missing cost-year, fed_om_cat and nonfed_om_cost and the pump office and
        // near gage office.

        return builder.build();
    }

    public static String getLocationId(String base, String sub) {
        boolean hasSub = sub != null && !sub.isEmpty();
        return hasSub ? base + "-" + sub : base;
    }

    public Projects retrieveProjectsFromView(String cursor, int pageSize, String projectIdMask,
                                             String office) {

        Condition whereClause =
                JooqDao.caseInsensitiveLikeRegexNullTrue(AV_PROJECT.AV_PROJECT.PROJECT_ID,
                        projectIdMask);
        if (office != null) {
            whereClause = whereClause.and(AV_PROJECT.AV_PROJECT.OFFICE_ID.eq(office));
        }

        String cursorOffice = null;
        String cursorProjectId = null;
        int total = 0;
        if (cursor == null || cursor.isEmpty()) {
            SelectConditionStep<Record1<Integer>> count =
                    dsl.select(count(asterisk()))
                            .from(AV_PROJECT.AV_PROJECT)
                            .where(whereClause);
            total = count.fetchOne().value1();
        } else {
            String[] parts = CwmsDTOPaginated.decodeCursor(cursor);
            if (parts.length == 4) {
                cursorOffice = parts[0];
                cursorProjectId = parts[1];
                pageSize = Integer.parseInt(parts[2]);
                total = Integer.parseInt(parts[3]);
            }
        }

        Condition pagingCondition = noCondition();
        if (cursorOffice != null || cursorProjectId != null) {
            Condition inSameOffice = AV_PROJECT.AV_PROJECT.OFFICE_ID.eq(cursorOffice)
                    .and(AV_PROJECT.AV_PROJECT.PROJECT_ID.gt(cursorProjectId));
            Condition nextOffice = AV_PROJECT.AV_PROJECT.OFFICE_ID.gt(cursorOffice);
            pagingCondition = inSameOffice.or(nextOffice);
        }

        SelectLimitPercentStep<usace.cwms.db.jooq.codegen.tables.records.AV_PROJECT> query =
                dsl.selectFrom(AV_PROJECT.AV_PROJECT)
                        .where(whereClause.and(pagingCondition))
                        .orderBy(AV_PROJECT.AV_PROJECT.OFFICE_ID, AV_PROJECT.AV_PROJECT.PROJECT_ID)
                        .limit(pageSize);

        List<Project> projs = query.fetch().map(ProjectDao::buildProject);

        Projects.Builder builder = new Projects.Builder(cursor, pageSize, total);
        builder.addAll(projs);
        return builder.build();
    }

    public Projects retrieveProjectsFromTable(String cursor, int pageSize,
                                              @Nullable String projectIdMask,
                                              @Nullable String office) {
        final String cursorOffice;
        final String cursorProjectId;
        int total;
        if (cursor == null || cursor.isEmpty()) {
            cursorOffice = null;
            cursorProjectId = null;

            Condition whereClause =
                    JooqDao.caseInsensitiveLikeRegexNullTrue(AV_PROJECT.AV_PROJECT.PROJECT_ID,
                            projectIdMask);
            if (office != null) {
                whereClause = whereClause.and(AV_PROJECT.AV_PROJECT.OFFICE_ID.eq(office));
            }

            SelectConditionStep<Record1<Integer>> count =
                    dsl.select(count(asterisk()))
                            .from(AV_PROJECT.AV_PROJECT)
                            .where(whereClause);

            total = count.fetchOne().value1();
        } else {
            cursorOffice = Projects.getOffice(cursor);
            cursorProjectId = Projects.getId(cursor);
            pageSize = Projects.getPageSize(cursor);
            total = Projects.getTotal(cursor);
        }

        // There are lots of ways the variables can be null or not so we need to build the query
        // based on the parameters.
        String query = buildTableQuery(projectIdMask, office, cursorOffice, cursorProjectId);

        int finalPageSize = pageSize;
        List<Project> projs = connectionResult(dsl, c -> {
            List<Project> projects;
            try (PreparedStatement ps = c.prepareStatement(query)) {
                fillTableQueryParameters(ps, projectIdMask, office, cursorOffice, cursorProjectId
                        , finalPageSize);

                try (ResultSet resultSet = ps.executeQuery()) {
                    projects = new ArrayList<>();
                    while (resultSet.next()) {
                        Project built = buildProjectFromTableRow(resultSet);
                        projects.add(built);
                    }
                }
            }
            return projects;
        });

        Projects.Builder builder = new Projects.Builder(cursor, pageSize, total);
        builder.addAll(projs);
        return builder.build();
    }

    private Project buildProjectFromTableRow(ResultSet resultSet) throws SQLException {
        Project.Builder builder = new Project.Builder();
        builder.withOfficeId(resultSet.getString(OFFICE_ID));
        builder.withName(resultSet.getString(PROJECT_ID));
        builder.withAuthorizingLaw(resultSet.getString(AUTHORIZING_LAW));
        builder.withProjectOwner(resultSet.getString(PROJECT_OWNER));
        builder.withHydropowerDesc(resultSet.getString(HYDROPOWER_DESCRIPTION));
        builder.withSedimentationDesc(resultSet.getString(SEDIMENTATION_DESCRIPTION));
        builder.withDownstreamUrbanDesc(resultSet.getString(DOWNSTREAM_URBAN_DESCRIPTION));
        builder.withBankFullCapacityDesc(resultSet.getString(BANK_FULL_CAPACITY_DESCRIPTION));
        builder.withPumpBackLocation(
                new Location.Builder(resultSet.getString(PUMP_BACK_OFFICE_ID),
                        resultSet.getString(PUMP_BACK_LOCATION_ID))
                        .build()
        );

        builder.withNearGageLocation(
                new Location.Builder(resultSet.getString(NEAR_GAGE_OFFICE_ID),
                        resultSet.getString(NEAR_GAGE_LOCATION_ID))
                        .build()
        );

        builder.withProjectRemarks(resultSet.getString(PROJECT_REMARKS));

        BigDecimal federalCost = resultSet.getBigDecimal(FEDERAL_COST);
        if (federalCost != null) {
            builder.withFederalCost(federalCost.doubleValue());
        }

        BigDecimal nonfederalCost = resultSet.getBigDecimal(NONFEDERAL_COST);
        if (nonfederalCost != null) {
            builder.withNonFederalCost(nonfederalCost.doubleValue());
        }

        BigDecimal federalOmCost = resultSet.getBigDecimal(FEDERAL_OM_COST);
        if (federalOmCost != null) {
            builder.withFederalOAndMCost(federalOmCost.doubleValue());
        }
        BigDecimal nonfederalOmCost = resultSet.getBigDecimal(NONFEDERAL_OM_COST);
        if (nonfederalOmCost != null) {
            builder.withNonFederalOAndMCost(nonfederalOmCost.doubleValue());
        }

        Timestamp costStamp = resultSet.getTimestamp(COST_YEAR, UTC_CALENDAR);
        if (costStamp != null) {
            builder.withCostYear(costStamp.toInstant());
        }

        Timestamp yieldTimeFrameStart = resultSet.getTimestamp(YIELD_TIME_FRAME_START,
                UTC_CALENDAR);
        if (yieldTimeFrameStart != null) {
            builder.withYieldTimeFrameStart(yieldTimeFrameStart.toInstant());
        }
        Timestamp yieldTimeFrameEnd = resultSet.getTimestamp(YIELD_TIME_FRAME_END, UTC_CALENDAR);
        if (yieldTimeFrameEnd != null) {
            builder.withYieldTimeFrameEnd(yieldTimeFrameEnd.toInstant());
        }

        return builder.build();
    }

    private void fillTableQueryParameters(PreparedStatement ps, String projectIdMask, String office,
                                          String cursorOffice, String cursorProjectId,
                                          int finalPageSize) throws SQLException {
        int index = 1;
        if (projectIdMask != null) {
            ps.setString(index++, projectIdMask);
        }

        if (office != null) {
            ps.setString(index++, office);
        }

        if (cursorOffice != null) {
            ps.setString(index++, cursorOffice);
            ps.setString(index++, cursorProjectId);
            ps.setString(index++, cursorOffice);  // its in there twice....
        }

        ps.setInt(index, finalPageSize);
    }

    private static String buildTableQuery(@Nullable String projectIdMask, @Nullable String office,
                                          String cursorOffice, String cursorProjectId) {
        String sql = SELECT_PART;

        if (projectIdMask != null || office != null || cursorOffice != null || cursorProjectId != null) {
            sql += "where (";

            if (projectIdMask != null && office != null) {
                sql += "(regexp_like(project.location_id, ?, 'i'))\n"  // projectIdMask
                        + " and office_id = ?\n";
            } else if (projectIdMask != null) {
                sql += "(regexp_like(project.location_id, ?, 'i'))\n";  // projectIdMask
            } else if (office != null) {
                sql += "office_id = ?\n";          // office
            }

            if (cursorOffice != null || cursorProjectId != null) {
                sql += " and (\n"
                        + "    (\n"
                        + "      OFFICE_ID = ?\n"  // cursorOffice
                        + "      and project.location_id > ?\n" //cursorProjectId
                        + "    )\n"
                        + "    or OFFICE_ID > ?\n"  // cursorOffice
                        + "  )\n";
            }

            sql += ")\n";
        }

        sql += "order by office_id, project_id fetch next ? rows only";  // pageSize

        return sql;
    }

    public void create(Project project) {
        boolean failIfExists = true;
        String office = project.getOfficeId();

        PROJECT_OBJ_T projectT = toProjectT(project);
        connection(dsl,
                c -> CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(getDslContext(c, office).configuration(),
                projectT, OracleTypeMap.formatBool(failIfExists)));
    }

    public static LOCATION_REF_T toLocationRefT(String base, String sub, String office) {
        return new LOCATION_REF_T(base, sub, office);
    }


    private static @NotNull LOCATION_REF_T getLocationRefT(String locationId, String office) {
        String base;
        String sub;
        if (locationId == null) {
            base = null;
            sub = null;
        } else {
            int fieldIndex = locationId.indexOf("-");
            if (fieldIndex == -1) {
                base = locationId;
                sub = null;
            } else {
                base = locationId.substring(0, fieldIndex);
                sub = locationId.substring(fieldIndex + 1);
            }
        }

        return toLocationRefT(base, sub, office);
    }


    public void store(Project project, boolean failIfExists) {
        String office = project.getOfficeId();

        PROJECT_OBJ_T projectT = toProjectT(project);
        connection(dsl,
                c -> CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(getDslContext(c, office).configuration(),
                projectT, OracleTypeMap.formatBool(failIfExists)));

    }

    public void update(Project project) {
        String office = project.getOfficeId();
        Project existingProject = retrieveProject(office, project.getName());
        if (existingProject == null) {
            throw new NotFoundException("Could not find project to update.");
        }

        PROJECT_OBJ_T projectT = toProjectT(project);
        connection(dsl,
                c -> CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(getDslContext(c, office).configuration(),
                projectT, OracleTypeMap.formatBool(false)));

    }

    private PROJECT_OBJ_T toProjectT(Project project) {
        LOCATION_OBJ_T projectLocation = new LOCATION_OBJ_T();
        projectLocation.setLOCATION_REF(getLocationRefT(project.getName(), project.getOfficeId()));

        LOCATION_OBJ_T pumpBackLocation = null;
        Location pb = project.getPumpBackLocation();
        if (pb != null) {
            pumpBackLocation = new LOCATION_OBJ_T();
            pumpBackLocation.setLOCATION_REF(getLocationRefT(pb.getName(), pb.getOfficeId()));
        }

        LOCATION_OBJ_T nearGageLocation = null;
        Location ng = project.getNearGageLocation();
        if (ng != null) {
            nearGageLocation = new LOCATION_OBJ_T();
            nearGageLocation.setLOCATION_REF(getLocationRefT(ng.getName(), ng.getOfficeId()));
        }

        String authorizingLaw = project.getAuthorizingLaw();
        Timestamp costYear = project.getCostYear() != null
                ? Timestamp.from(project.getCostYear()) : null;
        BigDecimal federalCost = project.getFederalCost() != null
                ? BigDecimal.valueOf(project.getFederalCost()) : null;
        BigDecimal nonFederalCost = (project.getNonFederalCost() != null)
                ? BigDecimal.valueOf(project.getNonFederalCost()) : null;
        BigDecimal federalOandMCost = (project.getFederalOAndMCost() != null)
                ? BigDecimal.valueOf(project.getFederalOAndMCost()) : null;
        BigDecimal nonFederalOandMCost = (project.getNonFederalOAndMCost() != null)
                ? BigDecimal.valueOf(project.getNonFederalOAndMCost()) : null;
        String costUnitsId = project.getCostUnit();
        String remarks = project.getProjectRemarks();
        String projectOwner = project.getProjectOwner();
        String hydropowerDescription = project.getHydropowerDesc();
        String sedimentationDescription = project.getSedimentationDesc();
        String downstreamUrbanDescription = project.getDownstreamUrbanDesc();
        String bankFullCapacityDescription = project.getBankFullCapacityDesc();
        Timestamp yieldTimeFrameStartTimestamp = (project.getYieldTimeFrameStart() != null)
                ? Timestamp.from(project.getYieldTimeFrameStart()) : null;
        Timestamp yieldTimeFrameEndTimestamp = (project.getYieldTimeFrameEnd() != null)
                ? Timestamp.from(project.getYieldTimeFrameEnd()) : null;
        return new PROJECT_OBJ_T(projectLocation, pumpBackLocation, nearGageLocation,
                authorizingLaw, costYear, federalCost, nonFederalCost, federalOandMCost,
                nonFederalOandMCost, costUnitsId, remarks, projectOwner, hydropowerDescription,
                sedimentationDescription, downstreamUrbanDescription, bankFullCapacityDescription,
                yieldTimeFrameStartTimestamp, yieldTimeFrameEndTimestamp);
    }

    public void delete(String office, String id, DeleteRule deleteRule) {

        connection(dsl,
                c -> CWMS_PROJECT_PACKAGE.call_DELETE_PROJECT(getDslContext(c, office).configuration(),
                id, deleteRule.getRule(), office
        ));
    }


    public Number publishStatusUpdate(String pProjectId,
                                      String appId, String sourceId,
                                      String tsId, Timestamp start,
                                      Timestamp end, String office) {
        BigInteger startTime = toBigInteger(start);
        BigInteger endTime = toBigInteger(end);
        return connectionResult(dsl, c -> CWMS_PROJECT_PACKAGE.call_PUBLISH_STATUS_UPDATE(
                getDslContext(c, office).configuration(),
                pProjectId, appId, sourceId,
                tsId, startTime, endTime, office)
        );
    }

    protected static BigInteger toBigInteger(Timestamp timestamp) {
        BigInteger retval = null;
        if (timestamp != null) {
            retval = BigInteger.valueOf(timestamp.getTime());
        }

        return retval;
    }

    protected static BigInteger toBigInteger(Long value) {
        BigInteger retval = null;
        if (value != null) {
            retval = BigInteger.valueOf(value);
        }

        return retval;
    }

    protected static BigInteger toBigInteger(int revokeTimeout) {
        return BigInteger.valueOf(revokeTimeout);
    }


    public String requestLock(String projectId, String appId,
                              boolean revokeExisting, int revokeTimeout, String office) {
        BigInteger revokeTimeoutBI = toBigInteger(revokeTimeout);
        return connectionResult(dsl, c -> CWMS_PROJECT_PACKAGE.call_REQUEST_LOCK(getDslContext(c,
                        office).configuration(),
                projectId, appId, OracleTypeMap.formatBool(revokeExisting), revokeTimeoutBI,
                office));
    }


    public void releaseLock(String lockId) {
        connection(dsl, c -> CWMS_PROJECT_PACKAGE.call_RELEASE_LOCK(
                DSL.using(c, SQLDialect.ORACLE18C).configuration(),
                lockId));
    }


    public void revokeLock(String projId, String appId,
                           int revokeTimeout, String office) {
        BigInteger revokeTimeoutBI = toBigInteger(revokeTimeout);
        connection(dsl, c ->
                CWMS_PROJECT_PACKAGE.call_REVOKE_LOCK(getDslContext(c, office).configuration(),
                        projId,
                        appId, revokeTimeoutBI, office));
    }


    public void denyLockRevocation(String lockId) {
        connection(dsl, c ->
                CWMS_PROJECT_PACKAGE.call_DENY_LOCK_REVOCATION(
                        DSL.using(c, SQLDialect.ORACLE18C).configuration(),
                        lockId));
    }


    public boolean isLocked(String projectId, String appId, String office) {
        String s = connectionResult(dsl, c -> CWMS_PROJECT_PACKAGE.call_IS_LOCKED(getDslContext(c
                        , office).configuration(),
                projectId, appId, office));
        return OracleTypeMap.parseBool(s);
    }


    public List<Lock> catLocks(String projMask, String appMask, TimeZone tz, String officeMask) throws SQLException {
        List<Lock> retval = new ArrayList<>();
        try (ResultSet resultSet = CWMS_PROJECT_PACKAGE.call_CAT_LOCKS(dsl.configuration(),
                        projMask, appMask, tz.getID(), officeMask)
                .intoResultSet()) {
            while (resultSet.next()) {
                String officeId = resultSet.getString("office_id");
                String projectId = resultSet.getString("project_id");
                String applicationId = resultSet.getString("application_id");
                String acquireTime = resultSet.getString("acquire_time");
                String sessionUser = resultSet.getString("session_user");
                String osUser = resultSet.getString("os_user");
                String sessionProgram = resultSet.getString("session_program");
                String sessionMachine = resultSet.getString("session_machine");

                Lock lock = new Lock(officeId, projectId, applicationId, acquireTime, sessionUser
                        , osUser, sessionProgram, sessionMachine);
                retval.add(lock);
            }
        }
        return retval;
    }

    public static class Lock {
        private final String officeId;
        private final String projectId;
        private final String applicationId;
        private final String acquireTime;
        private final String sessionUser;
        private final String osUser;
        private final String sessionProgram;
        private final String sessionMachine;

        public Lock(String officeId, String projectId, String applicationId, String acquireTime,
                    String sessionUser, String osUser, String sessionProgram,
                    String sessionMachine) {
            this.officeId = officeId;
            this.projectId = projectId;
            this.applicationId = applicationId;
            this.acquireTime = acquireTime;
            this.sessionUser = sessionUser;
            this.osUser = osUser;
            this.sessionProgram = sessionProgram;
            this.sessionMachine = sessionMachine;
        }

        public String getOfficeId() {
            return officeId;
        }

        public String getProjectId() {
            return projectId;
        }

        public String getApplicationId() {
            return applicationId;
        }

        public String getAcquireTime() {
            return acquireTime;
        }

        public String getSessionUser() {
            return sessionUser;
        }

        public String getOsUser() {
            return osUser;
        }

        public String getSessionProgram() {
            return sessionProgram;
        }

        public String getSessionMachine() {
            return sessionMachine;
        }
    }


    public void updateLockRevokerRights(LockRevokerRights lock, boolean allow) {
        CWMS_PROJECT_PACKAGE.call_UPDATE_LOCK_REVOKER_RIGHTS(dsl.configuration(),
                lock.getUserId(), lock.getProjectId(), OracleTypeMap.formatBool(allow),
                lock.getApplicationId(), lock.getOfficeId());
    }


    public List<LockRevokerRights> catLockRevokerRights(String projectMask,
                                                        String applicationMask, String officeMask) {
        return connectionResult(dsl, c -> {
            List<LockRevokerRights> retval = new ArrayList<>();
            try (ResultSet rs = CWMS_PROJECT_PACKAGE.call_CAT_LOCK_REVOKER_RIGHTS(
                    DSL.using(c, SQLDialect.ORACLE18C).configuration(),
                    projectMask, applicationMask, officeMask).intoResultSet()) {
                while (rs.next()) {
                    String officeId = rs.getString("office_id");
                    String projectId = rs.getString("project_id");
                    String applicationId = rs.getString("application_id");
                    String userId = rs.getString("user_id");

                    LockRevokerRights lock = new LockRevokerRights(officeId, projectId,
                            applicationId, userId);
                    retval.add(lock);
                }

            }
            return retval;
        });
    }


    public static class LockRevokerRights {
        private final String officeId;
        private final String projectId;
        private final String applicationId;
        private final String userId;

        public LockRevokerRights(String officeId, String projectId, String applicationId,
                                 String userId) {
            this.officeId = officeId;
            this.projectId = projectId;
            this.applicationId = applicationId;
            this.userId = userId;
        }

        public String getOfficeId() {
            return officeId;
        }

        public String getProjectId() {
            return projectId;
        }

        public String getApplicationId() {
            return applicationId;
        }

        public String getUserId() {
            return userId;
        }
    }


}
