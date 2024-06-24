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

import cwms.cda.data.dao.location.kind.LocationUtil;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.project.Project;
import java.math.BigDecimal;
import java.sql.Timestamp;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_OBJ_T;

public class ProjectUtil {
    private ProjectUtil() {
        throw new AssertionError("Utility class");
    }

    public static PROJECT_OBJ_T toProjectT(Project project) {

        LOCATION_OBJ_T projectLocation = LocationUtil.getLocation(project.getLocation());
        LOCATION_OBJ_T pumpBackLocation = LocationUtil.getLocation(project.getPumpBackLocation());
        LOCATION_OBJ_T nearGageLocation = LocationUtil.getLocation(project.getNearGageLocation());

        String authorizingLaw = project.getAuthorizingLaw();
        Timestamp costYear = project.getCostYear() != null
                ? Timestamp.from(project.getCostYear()) : null;
        BigDecimal federalCost = project.getFederalCost();
        BigDecimal nonFederalCost = project.getNonFederalCost();
        BigDecimal federalOandMCost = project.getFederalOAndMCost();
        BigDecimal nonFederalOandMCost = project.getNonFederalOAndMCost();
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
        Location projectLoc = LocationUtil.getLocation(projectLocation);
        builder.withLocation(projectLoc);

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

        Location ngLoc = LocationUtil.getLocation(projectObjT.getNEAR_GAGE_LOCATION());
        builder = builder.withNearGageLocation(ngLoc);

        Location pbLoc = LocationUtil.getLocation(projectObjT.getPUMP_BACK_LOCATION());
        builder = builder.withPumpBackLocation(pbLoc);

        BigDecimal federalCost = projectObjT.getFEDERAL_COST();
        builder = builder.withFederalCost(federalCost);

        BigDecimal federalOandMCost = projectObjT.getFEDERAL_OM_COST();
        builder = builder.withFederalOAndMCost(federalOandMCost);

        BigDecimal nonFederalCost = projectObjT.getNONFEDERAL_COST();
        builder = builder.withNonFederalCost(nonFederalCost);

        BigDecimal nonFederalOandMCost = projectObjT.getNONFEDERAL_OM_COST();
        builder = builder.withNonFederalOAndMCost(nonFederalOandMCost);

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
