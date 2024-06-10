package cwms.cda.data.dao.project;

import static cwms.cda.data.dao.project.ProjectUtil.getProject;
import static cwms.cda.data.dao.project.ProjectUtil.toProjectT;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.noCondition;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.project.Projects;
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
import org.jetbrains.annotations.Nullable;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SelectConditionStep;
import org.jooq.SelectLimitPercentStep;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_PROJECT;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_OBJ_T;

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

        return projectObjT == null ? null : getProject(projectObjT);
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

    public static BigInteger toBigInteger(Timestamp timestamp) {
        BigInteger retval = null;
        if (timestamp != null) {
            retval = BigInteger.valueOf(timestamp.getTime());
        }

        return retval;
    }

    public static BigInteger toBigInteger(Long value) {
        BigInteger retval = null;
        if (value != null) {
            retval = BigInteger.valueOf(value);
        }

        return retval;
    }

    public static BigInteger toBigInteger(int revokeTimeout) {
        return BigInteger.valueOf(revokeTimeout);
    }




}
