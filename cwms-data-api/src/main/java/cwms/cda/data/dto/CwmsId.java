/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = CwmsId.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonPropertyOrder({"officeId", "name"})
public final class CwmsId extends CwmsDTO {

    private final String name;
    private final Map<String, String> properties;
    @JsonIgnore
    private final List<String> requiredProperties;

    public CwmsId(Builder builder) {
        super(builder.officeId);
        this.name = builder.name;
        this.properties = builder.properties;
        this.requiredProperties = builder.requiredProperties;
    }

    public static CwmsId buildCwmsId(String officeId, String name) {
        return new CwmsId.Builder()
            .withOfficeId(officeId)
            .withName(name)
            .build();
    }

    @Override
    protected void validateInternal(CwmsDTOValidator validator) {
        super.validateInternal(validator);
        validator.required(getOfficeId(), "office-id");
        validator.required(getName(), "name");
        for(String requiredProperty : requiredProperties) {
            validator.required(properties.get(requiredProperty), requiredProperty);
        }
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @JsonIgnore
    public String getProperty(String key) {
        return properties.get(key);
    }

    public static class Builder {
        private final Map<String, String> properties = new LinkedHashMap<>();
        @JsonIgnore
        private final List<String> requiredProperties = new ArrayList<>();
        private String officeId;
        private String name;

        public Builder withProperties(Map<String, String> properties) {
            this.properties.putAll(properties);
            return this;
        }

        @JsonIgnore
        final Builder withRequiredProperty(String key, String value) {
            this.properties.put(key, value);
            this.requiredProperties.add(key);
            return this;
        }

        @JsonAnySetter
        public Builder withProperty(String key, String value) {
            this.properties.put(key, value);
            return this;
        }

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        // ------ Known properties can have their builder methods added here to add to properties map------ //
        public Builder withKind(String kind) {
            this.properties.put("kind", kind);
            return this;
        }

        public Builder withBoundingOfficeId(String boundingOfficeId) {
            this.properties.put("bounding-office-id", boundingOfficeId);
            return this;
        }
        // ------ end of known property builders ------ //

        public CwmsId build() {
            return new CwmsId(this);
        }
    }
}