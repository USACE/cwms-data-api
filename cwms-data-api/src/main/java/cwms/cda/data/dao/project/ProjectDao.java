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

    private final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    public static final String SELECT_PART = "select project.office_id,\n"
            + "       project.location_id as project_id,\n"
            + "       project.COST_YEAR,\n"
            + "       project.federal_cost,\n"
            + "       project.nonfederal_cost,\n"
            + "       project.FEDERAL_OM_COST,\n"
            + "       project.NONFEDERAL_OM_COST,\n"
            + "       project.authorizing_law,\n"
            + "       project.project_owner,\n"
            + "       project.hydropower_description,\n"
            + "       project.sedimentation_description,\n"
            + "       project.downstream_urban_description,\n"
            + "       project.bank_full_capacity_description,\n"
            + "       pumpback.location_id as pump_back_location_id,\n"
            + "       pumpback.p_office_id as pump_back_office_id,\n"
            + "       neargage.location_id as near_gage_location_id,\n"
            + "       neargage.n_office_id as near_gage_office_id,\n"
            + "       project.yield_time_frame_start,\n"
            + "       project.yield_time_frame_end,\n"
            + "       project.project_remarks\n"
            + "from ( select o.office_id as office_id,\n"
            + "              bl.base_location_id\n"
            + "                  ||substr('-', 1, length(pl.sub_location_id))\n"
            + "                  ||pl.sub_location_id as location_id,\n"
            + "              p.COST_YEAR,\n"
            + "              p.federal_cost,\n"
            + "              p.nonfederal_cost,\n"
            + "              p.FEDERAL_OM_COST,\n"
            + "              p.NONFEDERAL_OM_COST,\n"
            + "              p.authorizing_law,\n"
            + "              p.project_owner,\n"
            + "              p.hydropower_description,\n"
            + "              p.sedimentation_description,\n"
            + "              p.downstream_urban_description,\n"
            + "              p.bank_full_capacity_description,\n"
            + "              p.pump_back_location_code,\n"
            + "              p.near_gage_location_code,\n"
            + "              p.yield_time_frame_start,\n"
            + "              p.yield_time_frame_end,\n"
            + "              p.project_remarks\n"
            + "       from cwms_20.cwms_office o,\n"
            + "            cwms_20.at_base_location bl,\n"
            + "            cwms_20.at_physical_location pl,\n"
            + "            cwms_20.at_project p\n"
            + "       where bl.db_office_code = o.office_code\n"
            + "         and pl.base_location_code = bl.base_location_code\n"
            + "         and p.project_location_code = pl.location_code\n"
            + "     ) project\n"
            + "         left outer join\n"
            + "     ( select pl.location_code,\n"
            + "              o.office_id as p_office_id,\n"
            + "              bl.base_location_id\n"
            + "                  ||substr('-', 1, length(pl.sub_location_id))\n"
            + "                  ||pl.sub_location_id as location_id\n"
            + "       from cwms_20.cwms_office o,\n"
            + "            cwms_20.at_base_location bl,\n"
            + "            cwms_20.at_physical_location pl,\n"
            + "            cwms_20.at_project p\n"
            + "       where bl.db_office_code = o.office_code\n"
            + "         and pl.base_location_code = bl.base_location_code\n"
            + "         and p.project_location_code = pl.location_code\n"
            + "     ) pumpback on pumpback.location_code = project.pump_back_location_code\n"
            + "         left outer join\n"
            + "     ( select pl.location_code,\n"
            + "              o.office_id as n_office_id,\n"
            + "              bl.base_location_id\n"
            + "                  ||substr('-', 1, length(pl.sub_location_id))\n"
            + "                  ||pl.sub_location_id as location_id\n"
            + "       from cwms_20.cwms_office o,\n"
            + "            cwms_20.at_base_location bl,\n"
            + "            cwms_20.at_physical_location pl,\n"
            + "            cwms_20.at_project p\n"
            + "       where bl.db_office_code = o.office_code\n"
            + "         and pl.base_location_code = bl.base_location_code\n"
            + "         and p.project_location_code = pl.location_code\n"
            + "     ) neargage on neargage.location_code = project.near_gage_location_code\n";


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
        builder.withOfficeId(resultSet.getString("office_id"));
        builder.withName(resultSet.getString("project_id"));
        builder.withAuthorizingLaw(resultSet.getString("authorizing_law"));
        builder.withProjectOwner(resultSet.getString("project_owner"));
        builder.withHydropowerDesc(resultSet.getString("hydropower_description"));
        builder.withSedimentationDesc(resultSet.getString("sedimentation_description"));
        builder.withDownstreamUrbanDesc(resultSet.getString("downstream_urban_description"));
        builder.withBankFullCapacityDesc(resultSet.getString("bank_full_capacity_description"));
        builder.withPumpBackLocation(
                new Location.Builder(resultSet.getString("pump_back_office_id"),
                        resultSet.getString("pump_back_location_id"))
                        .build()
        );

        builder.withNearGageLocation(
                new Location.Builder(resultSet.getString("near_gage_office_id"),
                        resultSet.getString("near_gage_location_id"))
                        .build()
        );

        builder.withProjectRemarks(resultSet.getString("project_remarks"));

        BigDecimal federalCost = resultSet.getBigDecimal("federal_cost");
        if (federalCost != null) {
            builder.withFederalCost(federalCost.doubleValue());
        }

        BigDecimal nonfederalCost = resultSet.getBigDecimal("nonfederal_cost");
        if (nonfederalCost != null) {
            builder.withNonFederalCost(nonfederalCost.doubleValue());
        }

        BigDecimal federalOmCost = resultSet.getBigDecimal("FEDERAL_OM_COST");
        if (federalOmCost != null) {
            builder.withFederalOAndMCost(federalOmCost.doubleValue());
        }
        BigDecimal nonfederalOmCost = resultSet.getBigDecimal("NONFEDERAL_OM_COST");
        if (nonfederalOmCost != null) {
            builder.withNonFederalOAndMCost(nonfederalOmCost.doubleValue());
        }

        Timestamp costStamp = resultSet.getTimestamp("COST_YEAR", UTC_CALENDAR);
        if (costStamp != null) {
            builder.withCostYear(costStamp.toInstant());
        }

        Timestamp yieldTimeFrameStart = resultSet.getTimestamp("yield_time_frame_start",
                UTC_CALENDAR);
        if (yieldTimeFrameStart != null) {
            builder.withYieldTimeFrameStart(yieldTimeFrameStart.toInstant());
        }
        Timestamp yieldTimeFrameEnd = resultSet.getTimestamp("yield_time_frame_end", UTC_CALENDAR);
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
