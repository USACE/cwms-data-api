/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import org.jetbrains.annotations.Nullable;
import org.jooq.*;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_project.CAT_PROJECT;
import usace.cwms.db.jooq.codegen.tables.AV_PROJECT;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_OBJ_T;

public class ProjectDao extends JooqDao<Project> {
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

    /**
     * Retrieves a project based on the given office and project ID.
     *
     * @param office The office ID associated with the project.
     * @param projectId The project ID.
     * @return The retrieved project. Returns null if projectObjT is null.
     */
    public Project retrieveProject(String office, String projectId) {

        PROJECT_OBJ_T projectObjT = connectionResult(dsl, c -> {
                    Configuration conf = getDslContext(c, office).configuration();
                    return CWMS_PROJECT_PACKAGE.call_RETRIEVE_PROJECT(conf,
                            projectId, office);
                }
        );

        return projectObjT == null ? null : getProject(projectObjT);
    }

    /**
     * Retrieves projects based on the given parameters.
     *
     * @param cursor        The cursor to retrieve the next page of projects. If null or empty,
     *                      retrieves the first page.
     * @param office        The office ID to filter the projects by. Can be null.
     * @param projectIdMask The mask to match the project IDs against. Can be null.
     * @param pageSize      The number of projects to retrieve per page.
     * @return A Projects object containing the retrieved projects.
     */
    public Projects retrieveProjects(String cursor, @Nullable String office, @Nullable String projectIdMask, int pageSize) {
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
        String query = buildTableQuery(office, projectIdMask, cursorOffice != null || cursorProjectId != null);

        int finalPageSize = pageSize;
        List<Project> projs = connectionResult(dsl, c -> {
            List<Project> projects;
            try (PreparedStatement ps = c.prepareStatement(query)) {
                fillTableQueryParameters(ps, cursorOffice, cursorProjectId, office, projectIdMask, finalPageSize);

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
        builder.withFederalCost(federalCost);

        BigDecimal nonfederalCost = resultSet.getBigDecimal(NONFEDERAL_COST);
        builder.withNonFederalCost(nonfederalCost);

        BigDecimal federalOmCost = resultSet.getBigDecimal(FEDERAL_OM_COST);
        builder.withFederalOAndMCost(federalOmCost);

        BigDecimal nonfederalOmCost = resultSet.getBigDecimal(NONFEDERAL_OM_COST);
        builder.withNonFederalOAndMCost(nonfederalOmCost);

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

    private void fillTableQueryParameters(PreparedStatement ps, String cursorOffice, String cursorProjectId, String office, String projectIdMask,
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

    private static String buildTableQuery(@Nullable String office, @Nullable String projectIdMask,
                                          boolean useCursor) {
        String sql = SELECT_PART;

        if (projectIdMask != null || office != null || useCursor) {
            sql += " where (";

            if (projectIdMask != null && office != null) {
                sql += "(regexp_like(project.location_id, ?, 'i'))\n"  // projectIdMask
                        + " and office_id = ?\n";
            } else if (projectIdMask != null) {
                sql += "(regexp_like(project.location_id, ?, 'i'))\n";  // projectIdMask
            } else if (office != null) {
                sql += "office_id = ?\n";          // office
            }

            if (useCursor) {
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

    /**
     * Creates a new project.
     *
     * @param project The project object to be created.
     */
    public void create(Project project) {
        boolean failIfExists = true;
        String office = project.getLocation().getOfficeId();

        PROJECT_OBJ_T projectT = toProjectT(project);
        connection(dsl, c -> {
            Configuration conf = getDslContext(c, office).configuration();
            CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(conf,
            projectT, OracleTypeMap.formatBool(failIfExists));
        });
    }

    /**
     * Stores a project in the database.
     *
     * @param project The project to be stored.
     * @param failIfExists Flag indicating whether the storing operation should fail if the
     *                     project already exists. true if the operation should fail, false otherwise.
     */
    public void store(Project project, boolean failIfExists) {
        String office = project.getLocation().getOfficeId();

        PROJECT_OBJ_T projectT = toProjectT(project);
        connection(dsl, c -> {
            Configuration conf = getDslContext(c, office).configuration();
            CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(conf,
            projectT, OracleTypeMap.formatBool(failIfExists));
        });

    }

    /**
     * Updates a project in the database.
     *
     * @param project The project object containing the updated information.
     * @throws NotFoundException If the project to update is not found.
     */
    public void update(Project project) {
        String office = project.getLocation().getOfficeId();
        Project existingProject = retrieveProject(office, project.getLocation().getName());
        if (existingProject == null) {
            throw new NotFoundException("Could not find project to update.");
        }

        PROJECT_OBJ_T projectT = toProjectT(project);
        connection(dsl, c -> {
            Configuration conf = getDslContext(c, office).configuration();
            CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(conf,
            projectT, OracleTypeMap.formatBool(false));
        });

    }


    /**
     * Deletes a project based on the given office, project ID, and delete rule.
     *
     * @param office      The office ID associated with the project.
     * @param id          The project ID.
     * @param deleteRule  The delete rule specifying the deletion behavior.
     */
    public void delete(String office, String id, DeleteRule deleteRule) {
        connection(dsl, c -> {
            Configuration conf = getDslContext(c, office).configuration();
            CWMS_PROJECT_PACKAGE.call_DELETE_PROJECT(conf,
            id, deleteRule.getRule(), office);
        });
    }


    /**
     * Generates and publishes a message on the office's STATUS queue that a project has been
     * updated for a specified application.
     *
     * @param office        The text identifier of the office generating the message (and owning
     *                      the project).
     * @param projectId     The location identifier of the project that has been updated.
     * @param applicationId A text string identifying the application for which the update applies.
     * @param sourceId      An application-defined string of the instance and/or component that
     *                      generated the message. If NULL or not specified, the generated
     *                      message will not include this item.
     * @param tsId          A time series identifier of the time series associated with the
     *                      update. If NULL or not specified, the generated message will not
     *                      include this item.
     * @param start         The UTC start time of the updates to the time series, in Java
     *                      milliseconds. If NULL or not specified, the generated message will
     *                      not include this item.
     * @param end           The UTC end time of the updates to the time series, in Java
     *                      milliseconds. If NULL or not specified, the generated message will
     *                      not include this item.
     * @return The timestamp of the generated message
     */
    public Instant publishStatusUpdate(String office, String projectId, String applicationId,
                                      @Nullable String sourceId, @Nullable String tsId,
                                      @Nullable Instant start, @Nullable Instant end) {
        BigInteger startTime = toBigInteger(start);
        BigInteger endTime = toBigInteger(end);
        BigInteger millis = connectionResult(dsl, c -> {
            Configuration conf = getDslContext(c, office).configuration();
                    return CWMS_PROJECT_PACKAGE.call_PUBLISH_STATUS_UPDATE( conf,
                            projectId, applicationId, sourceId,
                            tsId, startTime, endTime, office);
                }
        );

        Instant retval = null;
        if (millis != null) {
            retval = Instant.ofEpochMilli(millis.longValue());
        }
        return retval;
    }

    @Nullable
    public static BigInteger toBigInteger(@Nullable Instant timestamp) {
        BigInteger retval = null;
        if (timestamp != null) {
            retval = BigInteger.valueOf(timestamp.toEpochMilli());
        }

        return retval;
    }


    /**
     * Retrieves the locations associated with a project.
     *
     * @param office The office ID associated with the project.
     * @return A list of Location objects representing the project's locations.
     */
    public List<Location> catProject(String office) {

        return connectionResult(dsl, c -> {
            Configuration conf = getDslContext(c, office).configuration();
            CAT_PROJECT catProject = CWMS_PROJECT_PACKAGE.call_CAT_PROJECT(conf, office);

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
            builder.withTimeZoneName(OracleTypeMap.toZoneId(timeZoneName, name));
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
