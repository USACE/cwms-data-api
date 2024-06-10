package cwms.cda.data.dao.project;

import static cwms.cda.data.dao.location.kind.LocationUtil.getLocationRef;

import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.project.Project;
import java.math.BigDecimal;
import java.sql.Timestamp;
import usace.cwms.db.dao.ifc.loc.LocationRefType;
import usace.cwms.db.dao.ifc.loc.LocationType;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_OBJ_T;
import usace.cwms.db.jooq.dao.util.LocationTypeUtil;

public class ProjectUtil {
    private ProjectUtil() {
        throw new AssertionError("Utility class");
    }

    public static PROJECT_OBJ_T toProjectT(Project project) {
        LOCATION_OBJ_T projectLocation = new LOCATION_OBJ_T();
        projectLocation.setLOCATION_REF(getLocationRef(project.getName(), project.getOfficeId()));

        LOCATION_OBJ_T pumpBackLocation = null;
        Location pb = project.getPumpBackLocation();
        if (pb != null) {
            pumpBackLocation = new LOCATION_OBJ_T();
            pumpBackLocation.setLOCATION_REF(getLocationRef(pb.getName(), pb.getOfficeId()));
        }

        LOCATION_OBJ_T nearGageLocation = null;
        Location ng = project.getNearGageLocation();
        if (ng != null) {
            nearGageLocation = new LOCATION_OBJ_T();
            nearGageLocation.setLOCATION_REF(getLocationRef(ng.getName(), ng.getOfficeId()));
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

    public static Project getProject(PROJECT_OBJ_T projectObjT) {
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

    public static String getLocationId(String base, String sub) {
        boolean hasSub = sub != null && !sub.isEmpty();
        return hasSub ? base + "-" + sub : base;
    }

}
