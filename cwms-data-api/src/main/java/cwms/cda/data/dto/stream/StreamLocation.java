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

package cwms.cda.data.dto.stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = StreamLocation.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class StreamLocation extends CwmsDTOBase {

    @JsonProperty(required = true)
    private final StreamLocationNode streamLocationNode; //the node representation of this location containing the loc id, stream id, bank, and station
    private final Double publishedStation;
    private final Double navigationStation;
    private final Double lowestMeasurableStage;
    private final Double totalDrainageArea;
    private final Double ungagedDrainageArea;
    private final String areaUnits;
    private final String stageUnits;

    private StreamLocation(Builder builder) {
        this.streamLocationNode = builder.streamLocationNode;
        this.publishedStation = builder.publishedStation;
        this.navigationStation = builder.navigationStation;
        this.lowestMeasurableStage = builder.lowestMeasurableStage;
        this.totalDrainageArea = builder.totalDrainageArea;
        this.ungagedDrainageArea = builder.ungagedDrainageArea;
        this.areaUnits = builder.areaUnits;
        this.stageUnits = builder.stageUnits;
    }

    public StreamLocationNode getStreamLocationNode() {
        return streamLocationNode;
    }

    public Double getPublishedStation() {
        return publishedStation;
    }

    public Double getNavigationStation() {
        return navigationStation;
    }

    public Double getLowestMeasurableStage() {
        return lowestMeasurableStage;
    }

    public Double getTotalDrainageArea() {
        return totalDrainageArea;
    }

    public Double getUngagedDrainageArea() {
        return ungagedDrainageArea;
    }

    public String getAreaUnits() {
        return areaUnits;
    }

    public String getStageUnits() {
        return stageUnits;
    }

    @JsonIgnore
    public CwmsId getId() {
        return getStreamLocationNode().getId();
    }

    @JsonIgnore
    public String getStationUnits() {
        return streamLocationNode.getStreamNode().getStationUnits();
    }

    @JsonIgnore
    public Double getStation() {
        return streamLocationNode.getStreamNode().getStation();
    }

    @JsonIgnore
    public CwmsId getStreamId() {
        return streamLocationNode.getStreamNode().getStreamId();
    }

    public static class Builder {
        private StreamLocationNode streamLocationNode;
        private Double publishedStation;
        private Double navigationStation;
        private Double lowestMeasurableStage;
        private Double totalDrainageArea;
        private Double ungagedDrainageArea;
        private String areaUnits;
        private String stageUnits;

        public Builder withStreamLocationNode(StreamLocationNode streamLocationNode) {
            this.streamLocationNode = streamLocationNode;
            return this;
        }

        public Builder withPublishedStation(Double publishedStation) {
            this.publishedStation = publishedStation;
            return this;
        }

        public Builder withNavigationStation(Double navigationStation) {
            this.navigationStation = navigationStation;
            return this;
        }

        public Builder withLowestMeasurableStage(Double lowestMeasurableStage) {
            this.lowestMeasurableStage = lowestMeasurableStage;
            return this;
        }

        public Builder withTotalDrainageArea(Double totalDrainageArea) {
            this.totalDrainageArea = totalDrainageArea;
            return this;
        }

        public Builder withUngagedDrainageArea(Double ungagedDrainageArea) {
            this.ungagedDrainageArea = ungagedDrainageArea;
            return this;
        }

        public Builder withAreaUnits(String areaUnits) {
            this.areaUnits = areaUnits;
            return this;
        }

        public Builder withStageUnits(String stageUnits) {
            this.stageUnits = stageUnits;
            return this;
        }

        public StreamLocation build() {
            return new StreamLocation(this);
        }
    }
}