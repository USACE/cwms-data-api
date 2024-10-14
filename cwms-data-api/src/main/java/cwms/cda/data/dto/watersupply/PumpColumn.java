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

import com.fasterxml.jackson.annotation.JsonIncludeProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonIncludeProperties("data-columns")
@JsonPropertyOrder({"0", "1", "2", "3"})
public final class PumpColumn {
    @JsonProperty(index = 0, value = "0")
    private final String pumpType;
    @JsonProperty(index = 1, value = "1")
    private final String transferTypeDisplay;
    @JsonProperty(index = 2, value = "2")
    private final String flow;
    @JsonProperty(index = 3, value = "3")
    private final String comment;

    public String getPumpType() {
        return pumpType;
    }

    public String getTransferTypeDisplay() {
        return transferTypeDisplay;
    }

    public String getFlow() {
        return flow;
    }

    public String getComment() {
        return comment;
    }

    public PumpColumn() {
        this.pumpType = "pump-type";
        this.transferTypeDisplay = "transfer-type-display";
        this.flow = "flow";
        this.comment = "comment";
    }
}
