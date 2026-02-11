package cwms.cda.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@JsonRootName("rating-specs")
@JsonDeserialize(builder = RatingSpecs.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class)
public class RatingSpecs extends CwmsDTOPaginated {

    private List<RatingSpec> specs;

    private RatingSpecs() {
    }

    private RatingSpecs(int offset, int pageSize, Integer total, List<RatingSpec> specsList) {
        super(Integer.toString(offset), pageSize, total);
        specs = new ArrayList<>(specsList);
    }

    @JacksonXmlElementWrapper(localName = "specs")
    @JacksonXmlProperty(localName = "rating-spec")
    public List<RatingSpec> getSpecs() {
        return Collections.unmodifiableList(specs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RatingSpecs that = (RatingSpecs) o;

        if (getPageSize() != that.getPageSize()) return false;
        if (getSpecs() != null ? !getSpecs().equals(that.getSpecs()) : that.getSpecs() != null) return false;
        if (getPage() != null ? !getPage().equals(that.getPage()) : that.getPage() != null) return false;
        if (getNextPage() != null ? !getNextPage().equals(that.getNextPage()) : that.getNextPage() != null)
            return false;
        return getTotal() != null ? getTotal().equals(that.getTotal()) : that.getTotal() == null;
    }

    @Override
    public int hashCode() {
        int result = getSpecs() != null ? getSpecs().hashCode() : 0;
        result = 31 * result + (getPage() != null ? getPage().hashCode() : 0);
        result = 31 * result + (getNextPage() != null ? getNextPage().hashCode() : 0);
        result = 31 * result + (getTotal() != null ? getTotal().hashCode() : 0);
        result = 31 * result + getPageSize();
        return result;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private int offset;
        private int pageSize;
        private Integer total;
        private List<RatingSpec> specs;

        public Builder() {
        }

        public Builder withPage(String page) {
            String[] parts = decodeCursor(page);
            if (parts.length > 0) {
                try {
                    this.offset = Integer.parseInt(parts[0]);
                } catch (NumberFormatException e) {
                    // Try different delimiter if default fails, though decodeCursor should handle it if base64
                }
            }
            return this;
        }

        public Builder withOffset(int offset) {
            this.offset = offset;
            return this;
        }

        public Builder withPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder withTotal(Integer total) {
            this.total = total;
            return this;
        }

        public Builder(int offset, int pageSize, Integer total) {
            this.offset = offset;
            this.pageSize = pageSize;
            this.total = total;
        }

        public Builder withSpecs(List<RatingSpec> specList) {
            this.specs = specList;
            return this;
        }

        public RatingSpecs build() {
            RatingSpecs retval = new RatingSpecs(offset, pageSize, total, specs);

            if (this.specs != null && this.specs.size() == this.pageSize) {
                String cursor = Integer.toString(offset + retval.specs.size());
                retval.nextPage = encodeCursor(cursor,
                        retval.pageSize,
                        retval.total);
            } else {
                retval.nextPage = null;
            }
            return retval;
        }

    }

}
