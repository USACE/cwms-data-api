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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.Location;

@JsonRootName("water_supply_pump")
@JsonDeserialize(builder = WaterSupplyPump.Builder.class)
public final class WaterSupplyPump extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final Location pumpLocation;
    @JsonProperty(required = true)
    private final PumpType pumpType;

    private WaterSupplyPump(Builder builder) {
        this.pumpLocation = builder.pumpLocation;
        this.pumpType = builder.pumpType;
    }

    public Location getPumpLocation() {
        return this.pumpLocation;
    }

    public PumpType getPumpType() {
        return this.pumpType;
    }

    public static class Builder {
        private Location pumpLocation;
        private PumpType pumpType;

        public Builder withPumpLocation(@JsonProperty("pump-location") Location pumpLocation) {
            this.pumpLocation = pumpLocation;
            return this;
        }

        public Builder withPumpType(@JsonProperty("pump-type") PumpType pumpType) {
            this.pumpType = pumpType;
            return this;
        }

        public WaterSupplyPump build() {
            return new WaterSupplyPump(this);
        }
    }
}
