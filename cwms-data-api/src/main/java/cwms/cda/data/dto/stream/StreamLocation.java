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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.LocationIdentifier;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

import java.util.Objects;

@FormattableWith(contentType = Formats.JSON, formatter = JsonV1.class)
@JsonDeserialize(builder = StreamLocation.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class StreamLocation implements CwmsDTOBase {

    private final LocationIdentifier streamLocationId;
    private final StreamJunctionIdentifier streamJunctionIdentifier;
    private final Double publishedStation;
    private final Double navigationStation;
    private final Double lowestMeasurableStage;
    private final Double totalDrainageArea;
    private final Double ungagedDrainageArea;

    private StreamLocation(Builder builder) {
        this.streamJunctionIdentifier = builder.streamJunctionIdentifier;
        this.streamLocationId = builder.streamLocationId;
        this.publishedStation = builder.publishedStation;
        this.navigationStation = builder.navigationStation;
        this.lowestMeasurableStage = builder.lowestMeasurableStage;
        this.totalDrainageArea = builder.totalDrainageArea;
        this.ungagedDrainageArea = builder.ungagedDrainageArea;
    }

    @Override
    public void validate() throws FieldException {
        if (this.streamLocationId == null) {
            throw new FieldException("The 'streamId' field of a StreamLocation cannot be null.");
        }
    }

    public StreamJunctionIdentifier getStreamJunctionId() {
        return streamJunctionIdentifier;
    }

    public LocationIdentifier getStreamLocationId() {
        return streamLocationId;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StreamLocation that = (StreamLocation) o;
        return Objects.equals(getStreamJunctionId(), that.getStreamJunctionId())
                && Objects.equals(getStreamLocationId(), that.getStreamLocationId())
                && Objects.equals(getPublishedStation(), that.getPublishedStation())
                && Objects.equals(getNavigationStation(), that.getNavigationStation())
                && Objects.equals(getLowestMeasurableStage(), that.getLowestMeasurableStage())
                && Objects.equals(getTotalDrainageArea(), that.getTotalDrainageArea())
                && Objects.equals(getUngagedDrainageArea(), that.getUngagedDrainageArea());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getStreamJunctionId(), getStreamLocationId(), getPublishedStation(),
                getNavigationStation(), getLowestMeasurableStage(), getTotalDrainageArea(), getUngagedDrainageArea());
    }

    public static class Builder {
        private LocationIdentifier streamLocationId;
        private StreamJunctionIdentifier streamJunctionIdentifier;
        private Double publishedStation;
        private Double navigationStation;
        private Double lowestMeasurableStage;
        private Double totalDrainageArea;
        private Double ungagedDrainageArea;

        public Builder withStreamLocationId(LocationIdentifier streamId) {
            this.streamLocationId = streamId;
            return this;
        }
        public Builder withStreamJunctionId(StreamJunctionIdentifier streamLocationIdentifier) {
            this.streamJunctionIdentifier = streamLocationIdentifier;
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

        public StreamLocation build() {
            return new StreamLocation(this);
        }
    }
}