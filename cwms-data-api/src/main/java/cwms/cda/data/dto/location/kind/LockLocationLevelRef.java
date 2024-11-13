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

package cwms.cda.data.dto.location.kind;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.util.Objects;

@JsonIgnoreProperties({"level-id", "office-id", "specified-level-id", "create-link"})
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
public class LockLocationLevelRef extends CwmsDTOBase {
    private final String levelLink;
    private final Double levelValue;

    @JsonCreator
    public LockLocationLevelRef(@JsonProperty("level-link") String levelLink, @JsonProperty("level-value") Double levelValue) {
        this.levelLink = levelLink;
        this.levelValue = levelValue;
    }

    public LockLocationLevelRef(String office, String locationLevelId, Double levelValue) {
        this.levelValue = levelValue;
        this.levelLink = createLink(office, locationLevelId);
    }

    public String getLevelLink() {
        return levelLink;
    }

    public Double getLevelValue() {
        return levelValue;
    }

    public String getOfficeId() {
        return levelLink.split("=")[1];
    }

    public String getLevelId() {
        return levelLink.split("/")[2].split("[?]")[0];
    }

    public String getSpecifiedLevelId() {
        String[] locationLevelId = getLevelId().split("[.]");
        return locationLevelId[locationLevelId.length - 1];
    }

    public static String createLink(String office, String locationLevelName) {
        return String.format("/locks/%s?office=%s", locationLevelName, office);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LockLocationLevelRef that = (LockLocationLevelRef) o;
        return Double.compare(getLevelValue(), that.getLevelValue()) == 0
                && Objects.equals(getLevelLink(), that.getLevelLink());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getLevelLink(), getLevelValue());
    }
}
