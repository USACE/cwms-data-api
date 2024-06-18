/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including but not limited to the rights
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

package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

import java.util.Objects;

@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class)
@JsonDeserialize(builder = Stream.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class Stream implements CwmsDTOBase {

    private final Boolean startsDownstream;
    private final String flowsIntoStream;
    private final Double flowsIntoStation;
    private final String flowsIntoBank;
    private final String divertsFromStream;
    private final Double divertsFromStation;
    private final String divertsFromBank;
    private final Double length;
    private final Double slope;
    private final String comment;
    private final LocationIdentifier locationIdentifier;

    private Stream(Builder builder) {
        this.startsDownstream = builder.startsDownstream;
        this.flowsIntoStream = builder.flowsIntoStream;
        this.flowsIntoStation = builder.flowsIntoStation;
        this.flowsIntoBank = builder.flowsIntoBank;
        this.divertsFromStream = builder.divertsFromStream;
        this.divertsFromStation = builder.divertsFromStation;
        this.divertsFromBank = builder.divertsFromBank;
        this.length = builder.length;
        this.slope = builder.slope;
        this.comment = builder.comment;
        this.locationIdentifier = builder.locationIdentifier;
    }

    @Override
    public void validate() throws FieldException {
        if (this.locationIdentifier == null) {
            throw new FieldException("The 'locationTemplate' field of a Stream cannot be null.");
        }
    }

    public Boolean getStartsDownstream() {
        return startsDownstream;
    }

    @JsonIgnore
    public String getOfficeId() {
        return locationIdentifier.getOfficeId();
    }

    public String getFlowsIntoStream() {
        return flowsIntoStream;
    }

    public Double getFlowsIntoStation() {
        return flowsIntoStation;
    }

    public String getFlowsIntoBank() {
        return flowsIntoBank;
    }

    public String getDivertsFromStream() {
        return divertsFromStream;
    }

    public Double getDivertsFromStation() {
        return divertsFromStation;
    }

    public String getDivertsFromBank() {
        return divertsFromBank;
    }

    public Double getLength() {
        return length;
    }

    public Double getSlope() {
        return slope;
    }

    public String getComment() {
        return comment;
    }

    @JsonIgnore
    public String getStreamId() {
        return locationIdentifier.getLocationId();
    }

    public LocationIdentifier getLocationIdentifier() {
        return locationIdentifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Stream that = (Stream) o;
        return Objects.equals(getStartsDownstream(), that.getStartsDownstream())
                && Objects.equals(getFlowsIntoStream(), that.getFlowsIntoStream())
                && Objects.equals(getFlowsIntoStation(), that.getFlowsIntoStation())
                && Objects.equals(getFlowsIntoBank(), that.getFlowsIntoBank())
                && Objects.equals(getDivertsFromStream(), that.getDivertsFromStream())
                && Objects.equals(getDivertsFromStation(), that.getDivertsFromStation())
                && Objects.equals(getDivertsFromBank(), that.getDivertsFromBank())
                && Objects.equals(getLength(), that.getLength())
                && Objects.equals(getSlope(), that.getSlope())
                && Objects.equals(getComment(), that.getComment())
                && Objects.equals(getLocationIdentifier(), that.getLocationIdentifier());
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(getStartsDownstream());
        result = 31 * result + Objects.hashCode(getFlowsIntoStream());
        result = 31 * result + Objects.hashCode(getFlowsIntoStation());
        result = 31 * result + Objects.hashCode(getFlowsIntoBank());
        result = 31 * result + Objects.hashCode(getDivertsFromStream());
        result = 31 * result + Objects.hashCode(getDivertsFromStation());
        result = 31 * result + Objects.hashCode(getDivertsFromBank());
        result = 31 * result + Objects.hashCode(getLength());
        result = 31 * result + Objects.hashCode(getSlope());
        result = 31 * result + Objects.hashCode(getComment());
        result = 31 * result + Objects.hashCode(getLocationIdentifier());
        return result;
    }

    public static class Builder {
        private Boolean startsDownstream;
        private String flowsIntoStream;
        private Double flowsIntoStation;
        private String flowsIntoBank;
        private String divertsFromStream;
        private Double divertsFromStation;
        private String divertsFromBank;
        private Double length;
        private Double slope;
        private String comment;
        private LocationIdentifier locationIdentifier;

        public Builder withStartsDownstream(Boolean startsDownstream) {
            this.startsDownstream = startsDownstream;
            return this;
        }

        public Builder withFlowsIntoStream(String flowsIntoStream) {
            this.flowsIntoStream = flowsIntoStream;
            return this;
        }

        public Builder withFlowsIntoStation(Double flowsIntoStation) {
            this.flowsIntoStation = flowsIntoStation;
            return this;
        }

        public Builder withFlowsIntoBank(String flowsIntoBank) {
            this.flowsIntoBank = flowsIntoBank;
            return this;
        }

        public Builder withDivertsFromStream(String divertsFromStream) {
            this.divertsFromStream = divertsFromStream;
            return this;
        }

        public Builder withDivertsFromStation(Double divertsFromStation) {
            this.divertsFromStation = divertsFromStation;
            return this;
        }

        public Builder withDivertsFromBank(String divertsFromBank) {
            this.divertsFromBank = divertsFromBank;
            return this;
        }

        public Builder withLength(Double length) {
            this.length = length;
            return this;
        }

        public Builder withSlope(Double slope) {
            this.slope = slope;
            return this;
        }

        public Builder withComment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder withLocationIdentifier(LocationIdentifier locationIdentifier) {
            this.locationIdentifier = locationIdentifier;
            return this;
        }

        public Stream build() {
            return new Stream(this);
        }
    }
}