/*
 *
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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

package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.json.JsonV2;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class,
    aliases = {Formats.JSON, Formats.DEFAULT})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(builder = CwmsIdLocationKind.Builder.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class CwmsIdLocationKind extends CwmsDTOBase {
    private final CwmsId location;
    private final String locationKindId;

    public CwmsIdLocationKind(Builder builder) {
        this.location = builder.id;
        this.locationKindId = builder.locationKindId;
    }

    public CwmsId getLocationId() {
        return location;
    }

    public String getLocationKindId() {
        return locationKindId;
    }

    public static final class Builder {
        private CwmsId id;
        private String locationKindId;

        public Builder withLocationId(CwmsId id) {
            this.id = id;
            return this;
        }

        public Builder withLocationKindId(String locationKindId) {
            this.locationKindId = locationKindId;
            return this;
        }

        public CwmsIdLocationKind build() {
            return new CwmsIdLocationKind(this);
        }
    }
}
