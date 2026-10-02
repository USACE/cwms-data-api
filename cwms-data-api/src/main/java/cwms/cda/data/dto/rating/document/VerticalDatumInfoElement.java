/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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

package cwms.cda.data.dto.rating.document;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import java.util.List;

@JsonDeserialize(builder = VerticalDatumInfoElement.Builder.class)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"office", "unit", "location", "native-datum", "local-datum-name", "elevation",
    "offset"})
public final class VerticalDatumInfoElement {

    @JsonProperty("office")
    @JacksonXmlProperty(isAttribute = true, localName = "office")
    private final String office;

    @JsonProperty("unit")
    @JacksonXmlProperty(isAttribute = true, localName = "unit")
    private final String unit;

    @JsonProperty("location")
    private final String location;

    @JsonProperty("native-datum")
    private final String nativeDatum;

    @JsonProperty("local-datum-name")
    private final String localDatumName;

    @JsonProperty("elevation")
    private final String elevation;

    @JsonProperty("offset")
    @JacksonXmlElementWrapper(useWrapping = false)
    private final List<VerticalDatumOffsetElement> offsets;

    private VerticalDatumInfoElement(Builder builder) {
        this.office = builder.office;
        this.unit = builder.unit;
        this.location = builder.location;
        this.nativeDatum = builder.nativeDatum;
        this.localDatumName = builder.localDatumName;
        this.elevation = builder.elevation;
        this.offsets = ListUtil.copyOf(builder.offsets);
    }

    public String getOffice() {
        return office;
    }

    public String getUnit() {
        return unit;
    }

    public String getLocation() {
        return location;
    }

    public String getNativeDatum() {
        return nativeDatum;
    }

    public String getLocalDatumName() {
        return localDatumName;
    }

    public String getElevation() {
        return elevation;
    }

    public List<VerticalDatumOffsetElement> getOffsets() {
        return offsets;
    }

    @JsonPOJOBuilder
    public static final class Builder {
        private String office;
        private String unit;
        private String location;
        private String nativeDatum;
        private String localDatumName;
        private String elevation;
        private List<VerticalDatumOffsetElement> offsets;

        @JsonProperty("office")
        public Builder withOffice(String office) {
            this.office = office;
            return this;
        }

        @JsonProperty("unit")
        public Builder withUnit(String unit) {
            this.unit = unit;
            return this;
        }

        @JsonProperty("location")
        public Builder withLocation(String location) {
            this.location = location;
            return this;
        }

        @JsonProperty("native-datum")
        public Builder withNativeDatum(String nativeDatum) {
            this.nativeDatum = nativeDatum;
            return this;
        }

        @JsonProperty("local-datum-name")
        public Builder withLocalDatumName(String localDatumName) {
            this.localDatumName = localDatumName;
            return this;
        }

        @JsonProperty("elevation")
        public Builder withElevation(String elevation) {
            this.elevation = elevation;
            return this;
        }

        @JsonProperty("offset")
        @JacksonXmlElementWrapper(useWrapping = false)
        public Builder withOffsets(List<VerticalDatumOffsetElement> offsets) {
            this.offsets = offsets;
            return this;
        }

        public VerticalDatumInfoElement build() {
            return new VerticalDatumInfoElement(this);
        }
    }
}
