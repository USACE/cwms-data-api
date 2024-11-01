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
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class,
        aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = WaterSupplyAccountingList.Builder.class)
public class WaterSupplyAccountingList extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final List<WaterSupplyAccounting> waterSupplyAccounting;
    @JsonProperty(required = true)
    private final int pageSize;

    private WaterSupplyAccountingList(Builder builder) {
        this.pageSize = builder.pageSize;
        this.waterSupplyAccounting = builder.waterSupplyAccounting;
    }

    public List<WaterSupplyAccounting> getWaterSupplyAccounting() {
        return this.waterSupplyAccounting;
    }

    public int getPageSize() {
        return this.pageSize;
    }

    public static final class Builder {
        private List<WaterSupplyAccounting> waterSupplyAccounting = new ArrayList<>();
        private int pageSize;

        public Builder withWaterSupplyAccounting(@NotNull List<WaterSupplyAccounting> waterSupplyAccounting) {
            this.waterSupplyAccounting.addAll(waterSupplyAccounting);
            return this;
        }

        public Builder withPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public WaterSupplyAccountingList build() {
            return new WaterSupplyAccountingList(this);
        }
    }
}
