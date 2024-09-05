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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import java.time.Instant;
import java.util.Map;


@JsonDeserialize(builder = PumpAccounting.Builder.class)
public final class PumpAccounting extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final CwmsId pumpLocation;
    @JsonProperty(required = true)
    private final Map<Instant, PumpTransfer> pumpTransfers;

    private PumpAccounting(Builder builder) {
        this.pumpLocation = builder.pumpLocation;
        this.pumpTransfers = builder.pumpTransfers;
    }

    public CwmsId getPumpLocation() {
        return this.pumpLocation;
    }

    public Map<Instant, PumpTransfer> getPumpTransfers() {
        return this.pumpTransfers;
    }

    public static final class Builder {
        private CwmsId pumpLocation;
        private Map<Instant, PumpTransfer> pumpTransfers;

        public Builder withPumpLocation(CwmsId pumpLocation) {
            this.pumpLocation = pumpLocation;
            return this;
        }

        public Builder withPumpTransfers(Map<Instant, PumpTransfer> pumpTransfers) {
            this.pumpTransfers = pumpTransfers;
            return this;
        }

        public PumpAccounting build() {
            return new PumpAccounting(this);
        }
    }
}
