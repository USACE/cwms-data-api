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

package cwms.cda.data.dto.stream;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = StreamNode.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class StreamNode implements CwmsDTOBase {

    private final CwmsId streamId;
    private final Bank bank;
    private final Double station;
    private final String stationUnits;

    private StreamNode(Builder builder) {
        this.streamId = builder.streamId;
        this.bank = builder.bank;
        this.station = builder.station;
        this.stationUnits = builder.stationUnits;
    }

    @Override
    public void validate() throws FieldException {
        if (this.streamId == null) {
            throw new FieldException("The 'locationIdentifier' field of a StreamLocationRef cannot be null.");
        }
        streamId.validate();
    }

    public CwmsId getStreamId() {
        return streamId;
    }

    public Bank getBank() {
        return bank;
    }

    public Double getStation() {
        return station;
    }

    public String getStationUnits() {
        return stationUnits;
    }

    public static class Builder {
        private CwmsId streamId;
        private Bank bank;
        private Double station;
        private String stationUnits;

        public Builder withStreamId(CwmsId cwmsId) {
            this.streamId = cwmsId;
            return this;
        }

        public Builder withBank(Bank bank) {
            this.bank = bank;
            return this;
        }

        public Builder withStation(Double station) {
            this.station = station;
            return this;
        }

        public Builder withStationUnits(String stationUnits) {
            this.stationUnits = stationUnits;
            return this;
        }

        public StreamNode build() {
            return new StreamNode(this);
        }
    }
}