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

package cwms.cda.data.dto.locationlevel;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.catalog.LocationAlias;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

@JsonDeserialize(builder = LocationLevelRef.Builder.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
public class LocationLevelRef extends CwmsDTOBase {

    @JsonProperty(required = true)
    @Schema(description = "Name of the location level")
    private final CwmsId locationLevelId;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private final SortedSet<String> aliases = new TreeSet<>();

    @Schema(description = "The date/time at which this location level configuration takes effect.")
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private final SortedSet<Instant> effectiveDates = new TreeSet<>();

    LocationLevelRef(LocationLevelRef.Builder builder) {
        effectiveDates.addAll(builder.effectiveDates);
        locationLevelId = builder.locationLevelId;
        aliases.addAll(builder.aliases);
    }

    public SortedSet<Instant> getEffectiveDates() {
        return effectiveDates;
    }

    public CwmsId getLocationLevelId() {
        return locationLevelId;
    }

    public SortedSet<String> getAliases() {
        return aliases;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private final List<String> aliases = new ArrayList<>();
        private final List<Instant> effectiveDates = new ArrayList<>();
        private CwmsId locationLevelId;

        public Builder withLevelDate(Instant effectiveDate) {
            this.effectiveDates.add(effectiveDate);
            return this;
        }

        public Builder withLocationLevelId(CwmsId locationLevelId) {
            this.locationLevelId = locationLevelId;
            return this;
        }

        public Builder withAlias(String alias) {
            if(alias != null) {
                this.aliases.add(alias);
            }
            return this;
        }

        public LocationLevelRef build()
        {
            return new LocationLevelRef(this);
        }
    }
}
