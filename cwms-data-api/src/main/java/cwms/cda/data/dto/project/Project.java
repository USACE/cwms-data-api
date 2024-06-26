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

package cwms.cda.data.dto.project;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.Location;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import java.math.BigDecimal;
import java.time.Instant;

@JsonDeserialize(builder = Project.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSON, formatter = JsonV2.class)
public class Project implements CwmsDTOBase {

    private final Location location;
    private final BigDecimal federalCost;
    private final BigDecimal nonFederalCost;
    private final Instant costYear;
    private final String costUnit;
    private final BigDecimal federalOAndMCost;
    private final BigDecimal nonFederalOAndMCost;
    private final String authorizingLaw;
    private final String projectOwner;
    private final String hydropowerDesc;
    private final String sedimentationDesc;
    private final String downstreamUrbanDesc;
    private final String bankFullCapacityDesc;
    private final Location pumpBackLocation;
    private final Location nearGageLocation;
    private final Instant yieldTimeFrameStart;
    private final Instant yieldTimeFrameEnd;
    private final String projectRemarks;


    private Project(Project.Builder builder) {

        this.location = builder.location;
        this.federalCost = builder.federalCost;
        this.nonFederalCost = builder.nonFederalCost;
        this.costYear = builder.costYear;
        this.costUnit = builder.costUnit;
        this.federalOAndMCost = builder.federalOAndMCost;
        this.nonFederalOAndMCost = builder.nonFederalOandMCost;
        this.authorizingLaw = builder.authorizingLaw;
        this.projectOwner = builder.projectOwner;
        this.hydropowerDesc = builder.hydropowerDesc;
        this.sedimentationDesc = builder.sedimentationDesc;
        this.downstreamUrbanDesc = builder.downstreamUrbanDesc;
        this.bankFullCapacityDesc = builder.bankFullCapacityDesc;
        this.pumpBackLocation = builder.pumpBackLocation;
        this.nearGageLocation = builder.nearGageLocation;
        this.yieldTimeFrameStart = builder.yieldTimeFrameStart;
        this.yieldTimeFrameEnd = builder.yieldTimeFrameEnd;
        this.projectRemarks = builder.projectRemarks;
    }

    @Override
    public void validate() throws FieldException {

    }

    public Location getLocation() {
        return location;
    }

    public String getAuthorizingLaw() {
        return authorizingLaw;
    }

    public String getBankFullCapacityDesc() {
        return bankFullCapacityDesc;
    }

    public String getDownstreamUrbanDesc() {
        return downstreamUrbanDesc;
    }

    public BigDecimal getFederalCost() {
        return federalCost;
    }

    public String getHydropowerDesc() {
        return hydropowerDesc;
    }

    public Location getNearGageLocation() {
        return nearGageLocation;
    }

    public BigDecimal getNonFederalCost() {
        return nonFederalCost;
    }

    public BigDecimal getFederalOAndMCost() {
        return federalOAndMCost;
    }

    public BigDecimal getNonFederalOAndMCost() {
        return nonFederalOAndMCost;
    }

    public Instant getCostYear() {
        return costYear;
    }

    public String getCostUnit() {
        return costUnit;
    }

    public String getProjectOwner() {
        return projectOwner;
    }

    public String getProjectRemarks() {
        return projectRemarks;
    }

    public Location getPumpBackLocation() {
        return pumpBackLocation;
    }

    public String getSedimentationDesc() {
        return sedimentationDesc;
    }

    public Instant getYieldTimeFrameEnd() {
        return yieldTimeFrameEnd;
    }

    public Instant getYieldTimeFrameStart() {
        return yieldTimeFrameStart;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private Location location;

        private BigDecimal federalCost;
        private BigDecimal nonFederalCost;
        private Instant costYear;
        private String costUnit;
        private BigDecimal federalOAndMCost;
        private BigDecimal nonFederalOandMCost;
        private String authorizingLaw;
        private String projectOwner;
        private String hydropowerDesc;
        private String sedimentationDesc;
        private String downstreamUrbanDesc;
        private String bankFullCapacityDesc;
        private Location pumpBackLocation;
        private Location nearGageLocation;
        private Instant yieldTimeFrameStart;
        private Instant yieldTimeFrameEnd;
        private String projectRemarks;


        public Project build() {
            return new Project(this);
        }

        /**
         * Copy the values from the given project into this builder.
         *
         * @param project the project to copy values from
         * @return this builder
         */
        public Builder from(Project project) {
            return this.withLocation(project.getLocation())
                    .withFederalCost(project.getFederalCost())
                    .withNonFederalCost(project.getNonFederalCost())
                    .withCostYear(project.getCostYear())
                    .withCostUnit(project.getCostUnit())
                    .withFederalOAndMCost(project.getFederalOAndMCost())
                    .withNonFederalOAndMCost(project.getNonFederalOAndMCost())
                    .withAuthorizingLaw(project.getAuthorizingLaw())
                    .withProjectOwner(project.getProjectOwner())
                    .withHydropowerDesc(project.getHydropowerDesc())
                    .withSedimentationDesc(project.getSedimentationDesc())
                    .withDownstreamUrbanDesc(project.getDownstreamUrbanDesc())
                    .withBankFullCapacityDesc(project.getBankFullCapacityDesc())
                    .withPumpBackLocation(project.getPumpBackLocation())
                    .withNearGageLocation(project.getNearGageLocation())
                    .withYieldTimeFrameStart(project.getYieldTimeFrameStart())
                    .withYieldTimeFrameEnd(project.getYieldTimeFrameEnd())
                    .withProjectRemarks(project.getProjectRemarks());
        }



        public Builder withLocation(Location location) {
            this.location = location;
            return this;
        }

        public Builder withFederalCost(BigDecimal federalCost) {
            this.federalCost = federalCost;
            return this;
        }

        public Builder withNonFederalCost(BigDecimal nonFederalCost) {
            this.nonFederalCost = nonFederalCost;
            return this;
        }

        public Builder withCostYear(Instant costYear) {
            this.costYear = costYear;
            return this;
        }

        public Builder withCostUnit(String costUnit) {
            this.costUnit = costUnit;
            return this;
        }

        public Builder withFederalOAndMCost(BigDecimal federalOandMCost) {
            this.federalOAndMCost = federalOandMCost;
            return this;
        }

        public Builder withNonFederalOAndMCost(BigDecimal nonFederalOandMCost) {
            this.nonFederalOandMCost = nonFederalOandMCost;
            return this;
        }

        public Builder withAuthorizingLaw(String authorizingLaw) {
            this.authorizingLaw = authorizingLaw;
            return this;
        }

        public Builder withProjectOwner(String projectOwner) {
            this.projectOwner = projectOwner;
            return this;
        }

        public Builder withHydropowerDesc(String hydropowerDesc) {
            this.hydropowerDesc = hydropowerDesc;
            return this;
        }

        public Builder withSedimentationDesc(String sedimentationDesc) {
            this.sedimentationDesc = sedimentationDesc;
            return this;
        }

        public Builder withDownstreamUrbanDesc(String downstreamUrbanDesc) {
            this.downstreamUrbanDesc = downstreamUrbanDesc;
            return this;
        }

        public Builder withBankFullCapacityDesc(String bankFullCapacityDesc) {
            this.bankFullCapacityDesc = bankFullCapacityDesc;
            return this;
        }

        public Builder withPumpBackLocation(Location pbLoc) {
            this.pumpBackLocation = pbLoc;
            return this;
        }

        public Builder withNearGageLocation(Location ngLoc) {
            this.nearGageLocation = ngLoc;
            return this;
        }

        public Builder withYieldTimeFrameStart(Instant yieldTimeFrameStart) {
            this.yieldTimeFrameStart = yieldTimeFrameStart;
            return this;
        }

        public Builder withYieldTimeFrameEnd(Instant yieldTimeFrameEnd) {
            this.yieldTimeFrameEnd = yieldTimeFrameEnd;
            return this;
        }

        public Builder withProjectRemarks(String projectRemarks) {
            this.projectRemarks = projectRemarks;
            return this;
        }

    }

}
