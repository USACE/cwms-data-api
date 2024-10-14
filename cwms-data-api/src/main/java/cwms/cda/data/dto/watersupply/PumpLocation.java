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

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsId;

@JsonDeserialize(builder = PumpLocation.Builder.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class PumpLocation {
    private final CwmsId pumpIn;
    private final CwmsId pumpOut;
    private final CwmsId pumpBelow;

    private PumpLocation(Builder builder) {
        this.pumpIn = builder.pumpIn;
        this.pumpOut = builder.pumpOut;
        this.pumpBelow = builder.pumpBelow;
    }

    public CwmsId getPumpIn() {
        return this.pumpIn;
    }

    public CwmsId getPumpOut() {
        return this.pumpOut;
    }

    public CwmsId getPumpBelow() {
        return this.pumpBelow;
    }

    public static final class Builder {
        private CwmsId pumpIn;
        private CwmsId pumpOut;
        private CwmsId pumpBelow;

        public Builder withPumpIn(CwmsId pumpIn) {
            this.pumpIn = pumpIn;
            return this;
        }

        public Builder withPumpOut(CwmsId pumpOut) {
            this.pumpOut = pumpOut;
            return this;
        }

        public Builder withPumpBelow(CwmsId pumpBelow) {
            this.pumpBelow = pumpBelow;
            return this;
        }

        public PumpLocation build() {
            return new PumpLocation(this);
        }
    }
}
