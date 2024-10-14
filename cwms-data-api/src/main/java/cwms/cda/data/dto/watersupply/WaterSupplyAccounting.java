/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.watersupply;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import io.swagger.v3.oas.annotations.media.Schema;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class,
        aliases = {Formats.DEFAULT, Formats.JSON})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = WaterSupplyAccounting.Builder.class)
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class WaterSupplyAccounting extends CwmsDTOPaginated {
    @JsonProperty(required = true)
    private final String contractName;
    @JsonProperty(required = true)
    private final WaterUser waterUser;
    @JsonProperty(required = true)
    private final PumpLocation pumpLocations;
    @JsonProperty(value = "data-columns")
    @Schema(name = "data-columns")
    private final PumpColumn pumpColumn;
    private final Map<Instant, List<PumpTransfer>> pumpAccounting;

    private WaterSupplyAccounting(Builder builder) {
        super(builder.page, builder.pageSize, builder.total);
        this.contractName = builder.contractName;
        this.waterUser = builder.waterUser;
        this.pumpLocations = builder.pumpLocations;
        this.pumpAccounting = builder.pumpAccounting;
        this.pumpColumn = new PumpColumn();
    }

    public String getContractName() {
        return this.contractName;
    }

    public WaterUser getWaterUser() {
        return this.waterUser;
    }

    public PumpColumn getPumpColumn() {
        return this.pumpColumn;
    }

    public Map<Instant, List<PumpTransfer>> getPumpAccounting() {
        return this.pumpAccounting;
    }

    public PumpLocation getPumpLocations() {
        return this.pumpLocations;
    }

    public static final class Builder {
        private String contractName;
        private WaterUser waterUser;
        private Map<Instant, List<PumpTransfer>> pumpAccounting;
        private PumpLocation pumpLocations;
        @JsonProperty(value = "data-columns")
        private PumpColumn pumpColumn;
        private String page;
        private int pageSize;
        private int total;

        public Builder withContractName(String contractName) {
            this.contractName = contractName;
            return this;
        }

        public Builder withWaterUser(WaterUser waterUser) {
            this.waterUser = waterUser;
            return this;
        }

        public Builder withPumpAccounting(
                Map<Instant, List<PumpTransfer>> pumpAccounting) {
            this.pumpAccounting = pumpAccounting;
            return this;
        }

        public Builder withPumpLocations(
                PumpLocation pumpLocations) {
            this.pumpLocations = pumpLocations;
            return this;
        }

        public Builder withPage(String page) {
            this.page = page;
            return this;
        }

        public Builder withPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder withTotal(int total) {
            this.total = total;
            return this;
        }

        public WaterSupplyAccounting build() {
            return new WaterSupplyAccounting(this);
        }
    }

    public void addTransfer(Timestamp dateTime, double flowValue, String transferTypeDisplay, String comment,
            PumpType pumpType, Timestamp previousDateTime) {
        if ((page == null || page.isEmpty()) && (pumpAccounting == null || pumpAccounting.isEmpty())) {
            page = encodeCursor(delimiter, String.format("%d", dateTime.getTime()), total);
        }
        if (pageSize > 0 && mapSize(pumpAccounting) == pageSize) {
            nextPage = encodeCursor(delimiter, String.format("%d", previousDateTime.getTime()), total);
        } else {
            assert pumpAccounting != null;
            pumpAccounting.computeIfAbsent(dateTime.toInstant(), k -> new ArrayList<>());
            pumpAccounting.get(dateTime.toInstant()).add(new PumpTransfer(pumpType, transferTypeDisplay,
                    flowValue, comment));
        }
    }

    public void addNullValue(Timestamp dateTime, int index) {
        pumpAccounting.get(dateTime.toInstant()).add(index, null);
    }

    private static int mapSize(Map<Instant, List<PumpTransfer>> map) {
        int size = 0;
        if (map == null) {
            return size;
        }
        for (List<PumpTransfer> list : map.values()) {
            size += list.size();
        }
        return size;
    }
}
