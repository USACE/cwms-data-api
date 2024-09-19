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
package cwms.cda.data.dto.measurement;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = UsgsMeasurement.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class UsgsMeasurement extends CwmsDTOBase
{

    private final String remarks;
    private final String currentRating;
    private final String controlCondition;
    private final Double shiftUsed;
    private final Double percentDifference;
    private final String flowAdjustment;
    private final Double deltaHeight;
    private final Double deltaTime;
    private final Double airTemp;
    private final Double waterTemp;

    private UsgsMeasurement(Builder builder)
    {
        this.remarks = builder.remarks;
        this.currentRating = builder.currentRating;
        this.controlCondition = builder.controlCondition;
        this.shiftUsed = builder.shiftUsed;
        this.percentDifference = builder.percentDifference;
        this.flowAdjustment = builder.flowAdjustment;
        this.deltaHeight = builder.deltaHeight;
        this.deltaTime = builder.deltaTime;
        this.airTemp = builder.airTemp;
        this.waterTemp = builder.waterTemp;
    }

    public String getRemarks()
    {
        return remarks;
    }

    public String getCurrentRating()
    {
        return currentRating;
    }

    public String getControlCondition()
    {
        return controlCondition;
    }

    public Double getShiftUsed()
    {
        return shiftUsed;
    }

    public Double getPercentDifference()
    {
        return percentDifference;
    }

    public String getFlowAdjustment()
    {
        return flowAdjustment;
    }

    public Double getDeltaHeight()
    {
        return deltaHeight;
    }

    public Double getDeltaTime()
    {
        return deltaTime;
    }

    public Double getAirTemp()
    {
        return airTemp;
    }

    public Double getWaterTemp()
    {
        return waterTemp;
    }

    public static final class Builder
    {
        private String remarks;
        private String currentRating;
        private String controlCondition;
        private Double shiftUsed;
        private Double percentDifference;
        private String flowAdjustment;
        private Double deltaHeight;
        private Double deltaTime;
        private Double airTemp;
        private Double waterTemp;

        public Builder withRemarks(String remarks)
        {
            this.remarks = remarks;
            return this;
        }

        public Builder withCurrentRating(String currentRating)
        {
            this.currentRating = currentRating;
            return this;
        }

        public Builder withControlCondition(String controlCondition)
        {
            this.controlCondition = controlCondition;
            return this;
        }

        public Builder withShiftUsed(Double shiftUsed)
        {
            this.shiftUsed = shiftUsed;
            return this;
        }

        public Builder withPercentDifference(Double percentDifference)
        {
            this.percentDifference = percentDifference;
            return this;
        }

        public Builder withFlowAdjustment(String flowAdjustment)
        {
            this.flowAdjustment = flowAdjustment;
            return this;
        }

        public Builder withDeltaHeight(Double deltaHeight)
        {
            this.deltaHeight = deltaHeight;
            return this;
        }

        public Builder withDeltaTime(Double deltaTime)
        {
            this.deltaTime = deltaTime;
            return this;
        }

        public Builder withAirTemp(Double airTemp)
        {
            this.airTemp = airTemp;
            return this;
        }

        public Builder withWaterTemp(Double waterTemp)
        {
            this.waterTemp = waterTemp;
            return this;
        }

        public UsgsMeasurement build()
        {
            return new UsgsMeasurement(this);
        }
    }
}
