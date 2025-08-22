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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class PumpTransfer extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final PumpType pumpType;
    @JsonProperty(required = true)
    private final String transferTypeDisplay;
    @JsonProperty(required = true)
    private final Double flow;
    @JsonProperty(required = true)
    private final String comment;

    @JsonCreator
    public PumpTransfer(@JsonProperty("pump-type") PumpType pumpType,
            @JsonProperty("transfer-type-display") String transferTypeDisplay,
            @JsonProperty("flow") Double flow, @JsonProperty("comment") String comment) {
        this.transferTypeDisplay = transferTypeDisplay;
        this.flow = flow;
        this.comment = comment;
        this.pumpType = pumpType;
    }

    public String getTransferTypeDisplay() {
        return this.transferTypeDisplay;
    }

    public Double getFlow() {
        return this.flow;
    }

    public PumpType getPumpType() {
        return this.pumpType;
    }

    public String getComment() {
        return this.comment;
    }

}
