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

package cwms.cda.data.dto.location.kind;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.data.dto.CwmsId;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = TurbineSetting.class, name = "turbine-setting")})
public abstract class Setting extends CwmsDTO {

    @JsonProperty(required = true)
    private final CwmsId locationId;

    protected Setting(Builder<?> builder) {
        //uses CwmsId instead of a separate office-id field
        super(null);
        this.locationId = builder.locationId;
    }

    @JsonIgnore
    @Override
    public String getOfficeId() {
        return locationId.getOfficeId();
    }

    public CwmsId getLocationId() {
        return locationId;
    }

    public abstract static class Builder<T extends Builder<T>> {
        private CwmsId locationId;

        public T withLocationId(CwmsId locationId) {
            this.locationId = locationId;
            return self();
        }

        // Subclasses must override this method to return "this"
        protected abstract T self();

        // Subclasses will override this method to return concrete object
        public abstract Setting build();
    }
}
