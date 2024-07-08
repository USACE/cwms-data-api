/*
 * MIT License
 * Copyright (c) 2024 Hydrologic Engineering Center
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.location.kind;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsDTOValidator;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = CompoundOutletRecord.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class CompoundOutletRecord extends CwmsDTO {
    @JsonProperty(required = true)
    private final CwmsId outletId;
    private final List<CwmsId> downstreamOutletIds = new ArrayList<>();

    private CompoundOutletRecord(Builder builder) {
        super(null);
        outletId = builder.outletId;
        if (builder.downstreamOutletIds != null) {
            downstreamOutletIds.addAll(builder.downstreamOutletIds);
        }
    }

    public List<CwmsId> getDownstreamOutletIds() {
        return new ArrayList<>(downstreamOutletIds);
    }

    public CwmsId getOutletId() {
        return outletId;
    }

    @Override
    protected void validateInternal(CwmsDTOValidator validator) {
        super.validateInternal(validator);
        validator.required(outletId, "outlet-id");
        validator.validateCollection(downstreamOutletIds);
    }

    public static final class Builder {
        @JsonProperty(required = true)
        private CwmsId outletId;
        private List<CwmsId> downstreamOutletIds;

        public Builder() {
        }

        public Builder(CompoundOutletRecord clone) {
            outletId = clone.outletId;
            downstreamOutletIds = new ArrayList<>(clone.downstreamOutletIds);
        }

        public CompoundOutletRecord build() {
            return new CompoundOutletRecord(this);
        }

        @JsonProperty(required = true)
        public Builder withOutletId(CwmsId outletId) {
            this.outletId = outletId;
            return this;
        }

        public Builder withDownstreamOutletIds(List<CwmsId> downstreamOutletIds) {
            this.downstreamOutletIds = downstreamOutletIds;
            return this;
        }
    }
}
