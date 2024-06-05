package cwms.cda.data.dao;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.noCondition;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.Project;
import cwms.cda.data.dto.Projects;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SelectConditionStep;
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
        Project.Builder builder = new Project.Builder()
                .withOfficeId(r.getOFFICE_ID())
                .withName(r.getPROJECT_ID());
        builder.withPumpBackLocationId(r.getPUMP_BACK_LOCATION_ID());
        builder.withNearGageLocationId(r.getNEAR_GAGE_LOCATION_ID());
        builder.withAuthorizingLaw(r.getAUTHORIZING_LAW());
        builder.withFederalCost(r.getFEDERAL_COST().doubleValue());
        builder.withNonFederalCost(r.getNONFEDERAL_COST().doubleValue());
        // TODO: This isn't fully built out yet.  need to figure out where
        // some of these fields come from
//                    builder.withFederalOAndMCost(r.getFEDERAL_OM_COST().doubleValue());
//                    builder.withNonFederalOAndMCost(r.getNONFEDERAL_OM_COST().doubleValue());

        return builder.build();
    }

    public static String getLocationId(String base, String sub) {
        boolean hasSub = sub != null && !sub.isEmpty();
        return hasSub ? base + "-" + sub : base;
    }

    public Projects retrieveProjects(String cursor, int pageSize, String projectIdMask, String office) {

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
        }

        List<Project> projs = dsl.selectFrom(AV_PROJECT.AV_PROJECT)
                .where(whereClause.and(pagingCondition))
                .orderBy(AV_PROJECT.AV_PROJECT.OFFICE_ID, AV_PROJECT.AV_PROJECT.PROJECT_ID)
                .limit(pageSize)
                .fetch()
                .map(r -> buildProject(r));

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
