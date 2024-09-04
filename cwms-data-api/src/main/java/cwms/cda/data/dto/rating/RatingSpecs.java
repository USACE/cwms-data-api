package cwms.cda.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@JsonDeserialize(builder = RatingSpecs.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
public class RatingSpecs extends CwmsDTOPaginated {

    private List<RatingSpec> specs;

    private RatingSpecs() {

    }

    private int offset;

    private RatingSpecs(int offset, int pageSize, Integer total, List<RatingSpec> specsList) {
        super(Integer.toString(offset), pageSize, total);
        specs = new ArrayList<>(specsList);
        this.offset = offset;
    }

    public List<RatingSpec> getSpecs() {
        return Collections.unmodifiableList(specs);
    }

    public static class Builder {
        private final int offset;
        private final int pageSize;
        private final Integer total;
        private List<RatingSpec> specs;

        public Builder(int offset, int pageSize, Integer total) {
            this.offset = offset;
            this.pageSize = pageSize;
            this.total = total;
        }

        public Builder specs(List<RatingSpec> specList) {
            this.specs = specList;
            return this;
        }

        public RatingSpecs build() {
            RatingSpecs retval = new RatingSpecs(offset, pageSize, total, specs);

            if (this.specs.size() == this.pageSize) {
                String cursor = Integer.toString(retval.offset + retval.specs.size());
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
