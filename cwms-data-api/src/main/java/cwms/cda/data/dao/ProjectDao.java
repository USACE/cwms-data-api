package cwms.cda.data.dao;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.noCondition;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.Project;
import cwms.cda.data.dto.Projects;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.logging.Logger;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SelectConditionStep;
import org.jooq.SelectLimitPercentStep;
import org.jooq.conf.ParamType;
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
    private static final Logger logger = Logger.getLogger(TimeSeriesDaoImpl.class.getName());

    private final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    private final String firstQuery =  "select project.office_id,\n"
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
            + "     ) neargage on neargage.location_code = project.near_gage_location_code\n"
            + "where ((regexp_like(project.location_id, ?, 'i')) and\n"
            + "       office_id = ?)\n"
            + "order by office_id, project_id fetch next ? rows only";

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
        LOCATION_OBJ_T projectLocation = projectObjT.getPROJECT_LOCATION();
        LOCATION_REF_T locRef = projectLocation.getLOCATION_REF();
        String office = locRef.getOFFICE_ID();
        String id = getLocationId(locRef.getBASE_LOCATION_ID(), locRef.getSUB_LOCATION_ID());

        String authorizingLaw = projectObjT.getAUTHORIZING_LAW();
        String bankFullCapacityDescription = projectObjT.getBANK_FULL_CAPACITY_DESCRIPTION();
        Timestamp costYear = projectObjT.getCOST_YEAR();
        String downstreamUrbanDescription = projectObjT.getDOWNSTREAM_URBAN_DESCRIPTION();
        BigDecimal federalCost = projectObjT.getFEDERAL_COST();
        String costUnitsId = projectObjT.getCOST_UNITS_ID();
        BigDecimal federalOandMCost = projectObjT.getFEDERAL_OM_COST();
        String hydropowerDescription = projectObjT.getHYDROPOWER_DESCRIPTION();
        LocationType neargageLocationType =
                LocationTypeUtil.toLocationType(projectObjT.getNEAR_GAGE_LOCATION());
        LocationRefType nearLocRef = null;
        if (neargageLocationType != null) {
            nearLocRef = neargageLocationType.getLocationRef();
        }
        BigDecimal nonFederalCost = projectObjT.getNONFEDERAL_COST();
        BigDecimal nonFederalOandMCost = projectObjT.getNONFEDERAL_OM_COST();
        String projectOwner = projectObjT.getPROJECT_OWNER();
        LocationType pumpbackLocationType =
                LocationTypeUtil.toLocationType(projectObjT.getPUMP_BACK_LOCATION());
        LocationRefType pumpbackLocRef = null;
        if (pumpbackLocationType != null) {
            pumpbackLocRef = pumpbackLocationType.getLocationRef();
        }
        String remarks = projectObjT.getREMARKS();
        String sedimentationDescription = projectObjT.getSEDIMENTATION_DESCRIPTION();
        Timestamp yieldTimeFrameEnd = projectObjT.getYIELD_TIME_FRAME_END();
        Timestamp yieldTimeFrameStart = projectObjT.getYIELD_TIME_FRAME_START();

        Project.Builder builder = new Project.Builder()
            .withOfficeId(office)
            .withName(id)
            .withAuthorizingLaw(authorizingLaw)
            .withBankFullCapacityDesc(bankFullCapacityDescription)
                .withHydropowerDesc(hydropowerDescription)
                .withProjectRemarks(remarks)
                .withSedimentationDesc(sedimentationDescription)
                .withProjectOwner(projectOwner)
                .withDownstreamUrbanDesc(downstreamUrbanDescription)
                .withCostUnit(costUnitsId)
                ;

        if (costYear != null) {
            builder = builder.withCostYear(costYear.toInstant());
        }

        if (federalCost != null) {
            builder = builder.withFederalCost(federalCost.doubleValue());
        }

        if (federalOandMCost != null) {
            builder = builder.withFederalOAndMCost(federalOandMCost.doubleValue());
        }

        if (nearLocRef != null) {
            builder = builder.withNearGageLocationId(nearLocRef.getOfficeId())
                    .withNearGageLocationId(getLocationId(nearLocRef.getBaseLocationId(), nearLocRef.getSubLocationId()));
        }
        if (nonFederalCost != null) {
            builder = builder.withNonFederalCost(nonFederalCost.doubleValue());
        }

        if (nonFederalOandMCost != null) {
            builder = builder.withNonFederalOAndMCost(nonFederalOandMCost.doubleValue());
        }

        if (pumpbackLocRef != null) {
            builder = builder.withPumpBackOfficeId(pumpbackLocRef.getOfficeId())
                .withPumpBackLocationId(getLocationId(pumpbackLocRef.getBaseLocationId(), pumpbackLocRef.getSubLocationId()));
        }

        if (yieldTimeFrameEnd != null) {
            builder = builder.withYieldTimeFrameEnd(yieldTimeFrameEnd.toInstant());
        }

        if (yieldTimeFrameStart != null) {
            builder = builder.withYieldTimeFrameStart(yieldTimeFrameStart.toInstant());
        }

        return builder.build();
    }

    private static Project buildProject(usace.cwms.db.jooq.codegen.tables.records.AV_PROJECT r) {
        Project.Builder builder = new Project.Builder();
        builder.withOfficeId(r.getOFFICE_ID());
        builder.withName(r.getPROJECT_ID());
        //builder.withPumpBackOfficeId(r.getPUMP_BACK_OFFICE_ID());
        builder.withPumpBackOfficeId(r.getOFFICE_ID());  // Can we assume same office?
        builder.withPumpBackLocationId(r.getPUMP_BACK_LOCATION_ID());
        //builder.withNearGageOfficeId(r.getNEAR_GAGE_OFFICE_ID());
        builder.withNearGageOfficeId(r.getOFFICE_ID());  // Can we assume same office?
        builder.withNearGageLocationId(r.getNEAR_GAGE_LOCATION_ID());
        builder.withAuthorizingLaw(r.getAUTHORIZING_LAW());
        builder.withProjectRemarks(r.getPROJECT_REMARKS());
        builder.withProjectOwner(r.getPROJECT_OWNER());
        builder.withHydropowerDesc(r.getHYDROPOWER_DESCRIPTION());
        builder.withSedimentationDesc(r.getSEDIMENTATION_DESCRIPTION());
        builder.withDownstreamUrbanDesc(r.getDOWNSTREAM_URBAN_DESCRIPTION());
        builder.withBankFullCapacityDesc(r.getBANK_FULL_CAPACITY_DESCRIPTION());
        BigDecimal federalCost = r.getFEDERAL_COST();
        if(federalCost != null) {
            builder.withFederalCost(federalCost.doubleValue());
        }
        BigDecimal nonfederalCost = r.getNONFEDERAL_COST();
        if(nonfederalCost != null) {
            builder.withNonFederalCost(nonfederalCost.doubleValue());
        }
        Timestamp yieldTimeFrameStart = r.getYIELD_TIME_FRAME_START();
        if(yieldTimeFrameStart != null) {
            builder.withYieldTimeFrameStart(yieldTimeFrameStart.toInstant());
        }
        Timestamp yieldTimeFrameEnd = r.getYIELD_TIME_FRAME_END();
        if(yieldTimeFrameEnd != null){
            builder.withYieldTimeFrameEnd(yieldTimeFrameEnd.toInstant());
        }

        // The view is missing cost-year, fed_om_cat and nonfed_om_cost and the pump office and near gage office.

        return builder.build();
    }

    public static String getLocationId(String base, String sub) {
        boolean hasSub = sub != null && !sub.isEmpty();
        return hasSub ? base + "-" + sub : base;
    }

    public Projects retrieveProjectsFromView(String cursor, int pageSize, String projectIdMask, String office) {

        Condition whereClause = JooqDao.caseInsensitiveLikeRegexNullTrue(AV_PROJECT.AV_PROJECT.PROJECT_ID, projectIdMask);
        if (office != null) {
            whereClause = whereClause.and(AV_PROJECT.AV_PROJECT.OFFICE_ID.eq(office));
        }

        String cursorOffice = null;
        String cursorProjectId = null;
        int total = 0;
        if (cursor == null || cursor.isEmpty()) {
            logger.info("where like:" + whereClause.toString());
            SelectConditionStep<Record1<Integer>> count =
                    dsl.select(count(asterisk()))
                            .from(AV_PROJECT.AV_PROJECT)
                            .where(whereClause);
            logger.info(() -> count.getSQL(ParamType.INLINED));
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

        SelectLimitPercentStep<usace.cwms.db.jooq.codegen.tables.records.AV_PROJECT> querry = dsl.selectFrom(AV_PROJECT.AV_PROJECT)
                .where(whereClause.and(pagingCondition))
                .orderBy(AV_PROJECT.AV_PROJECT.OFFICE_ID, AV_PROJECT.AV_PROJECT.PROJECT_ID)
                .limit(pageSize);

        logger.info(() -> querry.getSQL(ParamType.INLINED));

        List<Project> projs = querry.fetch().map(r -> buildProject(r));

        Projects.Builder builder = new Projects.Builder(cursor, pageSize, total);
        builder.addAll(projs);
        return builder.build();
    }

    public Projects retrieveProjectsFromTable(String cursor, int pageSize, String projectIdMask, String office) {


        Condition whereClause = JooqDao.caseInsensitiveLikeRegexNullTrue(AV_PROJECT.AV_PROJECT.PROJECT_ID, projectIdMask);
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

            // lets log this so we can see whhat it looks like
            logger.info(whereClause.and(pagingCondition).toString());
        }

        int finalPageSize = pageSize;
        List<Project> projs = connectionResult(dsl, c -> {
            List<Project> projects = null;
            try (PreparedStatement ps = c.prepareStatement(firstQuery)) {
                ps.setString(1, projectIdMask);
                ps.setString(2, office);
                ps.setInt(3, finalPageSize);

                try (ResultSet resultSet = ps.executeQuery()) {
                    projects = new ArrayList<>();
                    while (resultSet.next()) {
                        Project.Builder builder = new Project.Builder();
                        builder.withOfficeId(resultSet.getString("office_id"));
                        builder.withName(resultSet.getString("project_id"));
                        Timestamp costStamp = resultSet.getTimestamp("COST_YEAR", UTC_CALENDAR);
                        if(costStamp != null) {
                            builder.withCostYear(costStamp.toInstant());
                        }
                        BigDecimal federalCost = resultSet.getBigDecimal("federal_cost");
                        if(federalCost != null) {
                            builder.withFederalCost(federalCost.doubleValue());
                        }

                        BigDecimal nonfederalCost = resultSet.getBigDecimal("nonfederal_cost");
                        if(nonfederalCost != null) {
                            builder.withNonFederalCost(nonfederalCost.doubleValue());
                        }

                        BigDecimal federalOmCost = resultSet.getBigDecimal("FEDERAL_OM_COST");
                        if(federalOmCost != null) {
                            builder.withFederalOAndMCost(federalOmCost.doubleValue());
                        }
                        BigDecimal nonfederalOmCost = resultSet.getBigDecimal("NONFEDERAL_OM_COST");
                        if(nonfederalOmCost != null) {
                            builder.withNonFederalOAndMCost(nonfederalOmCost.doubleValue());
                        }
                        builder.withAuthorizingLaw(resultSet.getString("authorizing_law"));
                        builder.withProjectOwner(resultSet.getString("project_owner"));
                        builder.withHydropowerDesc(resultSet.getString("hydropower_description"));
                        builder.withSedimentationDesc(resultSet.getString("sedimentation_description"));
                        builder.withDownstreamUrbanDesc(resultSet.getString("downstream_urban_description"));
                        builder.withBankFullCapacityDesc(resultSet.getString("bank_full_capacity_description"));
                        builder.withPumpBackLocationId(resultSet.getString("pump_back_location_id"));
                        builder.withPumpBackOfficeId(resultSet.getString("pump_back_office_id"));
                        builder.withNearGageLocationId(resultSet.getString("near_gage_location_id"));
                        builder.withNearGageOfficeId(resultSet.getString("near_gage_office_id"));
                        Timestamp yieldTimeFrameStart = resultSet.getTimestamp("yield_time_frame_start", UTC_CALENDAR);
                        if (yieldTimeFrameStart != null) {
                            builder.withYieldTimeFrameStart(yieldTimeFrameStart.toInstant());
                        }
                        Timestamp yieldTimeFrameEnd = resultSet.getTimestamp("yield_time_frame_end", UTC_CALENDAR);
                        if(yieldTimeFrameEnd != null) {
                            builder.withYieldTimeFrameEnd(yieldTimeFrameEnd.toInstant());
                        }
                        builder.withProjectRemarks(resultSet.getString("project_remarks"));

                        projects.add(builder.build());
                    }
                }
            }
            return projects;
        });


        Projects.Builder builder = new Projects.Builder(cursor, pageSize, total);
        builder.addAll(projs);
        return builder.build();
    }

    public void create(Project project) {
        boolean failIfExists = true;
        String office = project.getOfficeId();

        PROJECT_OBJ_T projectT = toProjectT(project);
        connection(dsl, c -> CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(getDslContext(c, office).configuration(),
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
        connection(dsl, c -> CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(getDslContext(c, office).configuration(),
                projectT, OracleTypeMap.formatBool(failIfExists)));

    }

    public void update(Project project) {
        String office = project.getOfficeId();
        Project existingProject = retrieveProject(office, project.getName());
        if (existingProject == null) {
            throw new NotFoundException("Could not find project to update.");
        }

        PROJECT_OBJ_T projectT = toProjectT(project);
        connection(dsl, c -> CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(getDslContext(c, office).configuration(),
                projectT, OracleTypeMap.formatBool(false)));

    }

    private PROJECT_OBJ_T toProjectT(Project project) {
        LOCATION_OBJ_T projectLocation = new LOCATION_OBJ_T();
        projectLocation.setLOCATION_REF(getLocationRefT(project.getName(), project.getOfficeId()));

        LOCATION_OBJ_T pumpBackLocation = null;
        if (project.getPumpBackLocationId() != null && project.getPumpBackOfficeId() != null) {
            pumpBackLocation = new LOCATION_OBJ_T();
            pumpBackLocation.setLOCATION_REF(getLocationRefT(project.getPumpBackLocationId(),
                    project.getPumpBackOfficeId()));
        }


        LOCATION_OBJ_T nearGageLocation = null;
        if (project.getNearGageLocationId() != null && project.getNearGageOfficeId() != null) {
            nearGageLocation = new LOCATION_OBJ_T();
            nearGageLocation.setLOCATION_REF(getLocationRefT(project.getNearGageLocationId(),
                    project.getNearGageOfficeId()));
        }

        String authorizingLaw = project.getAuthorizingLaw();
        Timestamp costYear = project.getCostYear() == null ? null : Timestamp.from(project.getCostYear());
        BigDecimal federalCost = project.getFederalCost() == null ? null : BigDecimal.valueOf(project.getFederalCost());

        BigDecimal nonFederalCost = (project.getNonFederalCost() != null) ? BigDecimal.valueOf(project.getNonFederalCost()) : null;
        BigDecimal federalOandMCost = (project.getFederalOAndMCost() != null) ? BigDecimal.valueOf(project.getFederalOAndMCost()) : null;
        BigDecimal nonFederalOandMCost = (project.getNonFederalOAndMCost() != null) ? BigDecimal.valueOf(project.getNonFederalOAndMCost()) : null;
        String costUnitsId = project.getCostUnit();
        String remarks = project.getProjectRemarks();
        String projectOwner = project.getProjectOwner();
        String hydropowerDescription = project.getHydropowerDesc();
        String sedimentationDescription = project.getSedimentationDesc();
        String downstreamUrbanDescription = project.getDownstreamUrbanDesc();
        String bankFullCapacityDescription = project.getBankFullCapacityDesc();
        Timestamp yieldTimeFrameStartTimestamp = (project.getYieldTimeFrameStart() != null) ? Timestamp.from(project.getYieldTimeFrameStart()) : null;
        Timestamp yieldTimeFrameEndTimestamp = (project.getYieldTimeFrameEnd() != null) ? Timestamp.from(project.getYieldTimeFrameEnd()) : null;
        return new PROJECT_OBJ_T(projectLocation, pumpBackLocation, nearGageLocation,
                authorizingLaw, costYear, federalCost, nonFederalCost, federalOandMCost,
                nonFederalOandMCost, costUnitsId, remarks, projectOwner, hydropowerDescription,
                sedimentationDescription, downstreamUrbanDescription, bankFullCapacityDescription,
                yieldTimeFrameStartTimestamp, yieldTimeFrameEndTimestamp);
    }

    public void delete(String office, String id, DeleteRule deleteRule) {
//        Project project = retrieveProject(office, id);
//        if (project == null) {
//            throw new NotFoundException("Could not find project to delete.");
//        }

        connection(dsl, c -> CWMS_PROJECT_PACKAGE.call_DELETE_PROJECT(getDslContext(c, office).configuration(),
                id, deleteRule.getRule(), office
        ));
    }
}
