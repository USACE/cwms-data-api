package cwms.cda.data.dao.project;

import static cwms.cda.data.dao.project.ProjectUtil.getLocationId;
import static cwms.cda.data.dao.project.ProjectUtil.getProject;
import static cwms.cda.data.dao.project.ProjectUtil.toProjectT;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.logging.Logger;
import org.jetbrains.annotations.Nullable;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_project.CAT_PROJECT;
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

    // These are the columns from the PROJECT_CAT cursor
    public static final String DB_OFFICE_ID = "DB_OFFICE_ID";
    public static final String BASE_LOCATION_ID = "BASE_LOCATION_ID";
    public static final String SUB_LOCATION_ID = "SUB_LOCATION_ID";
    public static final String TIME_ZONE_NAME = "TIME_ZONE_NAME";
    public static final String LATITUDE = "LATITUDE";
    public static final String LONGITUDE = "LONGITUDE";
    public static final String HORIZONTAL_DATUM = "HORIZONTAL_DATUM";
    public static final String ELEVATION = "ELEVATION";
    public static final String ELEV_UNIT_ID = "ELEV_UNIT_ID";
    public static final String VERTICAL_DATUM = "VERTICAL_DATUM";
    public static final String PUBLIC_NAME = "PUBLIC_NAME";
    public static final String LONG_NAME = "LONG_NAME";
    public static final String DESCRIPTION = "DESCRIPTION";
    public static final String ACTIVE_FLAG = "ACTIVE_FLAG";

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
                fillTableQueryParameters(ps, projectIdMask, office, cursorOffice, cursorProjectId, finalPageSize);

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
        String prjOffice = resultSet.getString(OFFICE_ID);
        String prjId = resultSet.getString(PROJECT_ID);
        Location prjLoc = new Location.Builder(prjOffice, prjId)
                .withActive(null)
                .build();

        builder.withLocation(prjLoc);
        builder.withAuthorizingLaw(resultSet.getString(AUTHORIZING_LAW));
        builder.withProjectOwner(resultSet.getString(PROJECT_OWNER));
        builder.withHydropowerDesc(resultSet.getString(HYDROPOWER_DESCRIPTION));
        builder.withSedimentationDesc(resultSet.getString(SEDIMENTATION_DESCRIPTION));
        builder.withDownstreamUrbanDesc(resultSet.getString(DOWNSTREAM_URBAN_DESCRIPTION));
        builder.withBankFullCapacityDesc(resultSet.getString(BANK_FULL_CAPACITY_DESCRIPTION));

        String pbOffice = resultSet.getString(PUMP_BACK_OFFICE_ID);
        String pbId = resultSet.getString(PUMP_BACK_LOCATION_ID);
        if (pbOffice != null && pbId != null) {
            builder.withPumpBackLocation(
                    new Location.Builder(pbOffice, pbId)
                            .withActive(null)
                            .build()
            );
        }

        String ngOffice = resultSet.getString(NEAR_GAGE_OFFICE_ID);
        String ngId = resultSet.getString(NEAR_GAGE_LOCATION_ID);
        if (ngOffice != null && ngId != null) {
            builder.withNearGageLocation(
                    new Location.Builder(ngOffice, ngId)
                            .withActive(null)
                            .build()
            );
        }

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
            sql += " where (";

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
        String office = project.getLocation().getOfficeId();

        PROJECT_OBJ_T projectT = toProjectT(project);
        connection(dsl,
                c -> CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(getDslContext(c, office).configuration(),
                projectT, OracleTypeMap.formatBool(failIfExists)));
    }


    public void store(Project project, boolean failIfExists) {
        String office = project.getLocation().getOfficeId();

        PROJECT_OBJ_T projectT = toProjectT(project);
        connection(dsl,
                c -> CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(getDslContext(c, office).configuration(),
                projectT, OracleTypeMap.formatBool(failIfExists)));

    }

    public void update(Project project) {
        String office = project.getLocation().getOfficeId();
        Project existingProject = retrieveProject(office, project.getLocation().getName());
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


    public List<Location> catProject(String office) {

        return connectionResult(dsl, c -> {
            CAT_PROJECT catProject = CWMS_PROJECT_PACKAGE.call_CAT_PROJECT(getDslContext(c,
                    office).configuration(), office);

            // catProject has two open ResultSets.
            // Other places close the basin one we aren't using
            // FYI basinRs here is a MockResultSet
            // The MockResultSet.close() impl just sets its internal Result variable to null
            // So I suspect that with jOOQ we don't actually have to "close" anything here.
            ResultSet basinRs = catProject.getP_BASIN_CAT().intoResultSet();
            if (basinRs != null && !basinRs.isClosed()) {
                basinRs.close();
            }

            Result<Record> projectCatalog = catProject.getP_PROJECT_CAT();
            return projectCatalog.map(this::buildLocation);
        });
    }

    private Location buildLocation(Record r) {

        String office = r.get(DB_OFFICE_ID, String.class);

        String base = r.get(BASE_LOCATION_ID, String.class);
        String sub = r.get(SUB_LOCATION_ID, String.class);
        String name = getLocationId(base, sub);
        Location.Builder builder = new Location.Builder(office, name);

        String timeZoneName = r.get(TIME_ZONE_NAME, String.class);
        if (timeZoneName != null) {
            builder.withTimeZoneName(ZoneId.of(timeZoneName));
        }
        Double latitude = r.get(LATITUDE, Double.class);
        if (latitude != null) {
            builder.withLatitude(latitude);
        }
        Double longitude = r.get(LONGITUDE, Double.class);
        if (longitude != null) {
            builder.withLongitude(longitude);
        }
        String horizontalDatum = r.get(HORIZONTAL_DATUM, String.class);
        if (horizontalDatum != null) {
            builder.withHorizontalDatum(horizontalDatum);
        }
        builder.withElevation(r.get(ELEVATION, Double.class));
        builder.withElevationUnits(r.get(ELEV_UNIT_ID, String.class));
        builder.withVerticalDatum(r.get(VERTICAL_DATUM, String.class));
        builder.withPublicName(r.get(PUBLIC_NAME, String.class));
        builder.withLongName(r.get(LONG_NAME, String.class));
        builder.withDescription(r.get(DESCRIPTION, String.class));
        String activeStr = r.get(ACTIVE_FLAG, String.class);
        if (activeStr != null) {
            builder.withActive(OracleTypeMap.parseBool(activeStr));
        }

        return builder.build();
    }


}
