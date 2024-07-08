package cwms.cda.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@JsonDeserialize(builder = RatingTemplates.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
public class RatingTemplates extends CwmsDTOPaginated {

    private List<RatingTemplate> templates;

    private RatingTemplates() {
    }

    private int offset;

    private RatingTemplates(int offset, int pageSize, Integer total,
                            List<RatingTemplate> templates) {
        super(Integer.toString(offset), pageSize, total);
        this.templates = new ArrayList<>(templates);
        this.offset = offset;
    }

    public List<RatingTemplate> getTemplates() {
        return Collections.unmodifiableList(templates);
    }


    @Override
    public void validate() throws FieldException {

    }

    public static class Builder {
        private final int offset;
        private final int pageSize;
        private final Integer total;
        private List<RatingTemplate> templates;

        public Builder(int offset, int pageSize, Integer total) {
            this.offset = offset;
            this.pageSize = pageSize;
            this.total = total;
        }

        public Builder templates(List<RatingTemplate> specList) {
            this.templates = specList;
            return this;
        }

        private int getCount() {
            int count = 0;

            if (templates != null) {
                for (RatingTemplate template : templates) {
                    List<String> ratingIds = template.getRatingIds();
                    if (ratingIds != null && !ratingIds.isEmpty()) {
                        count += ratingIds.size();
                    } else {
                        // for purposes of paging a null or empty list counts as a row
                        count++;
                    }
                }
            }

            return count;
        }

        public RatingTemplates build() {
            RatingTemplates retval = new RatingTemplates(offset, pageSize, total, templates);

            int count = getCount();
            if (count >= this.pageSize) {
                String cursor = Integer.toString(retval.offset + count);
                retval.nextPage = encodeCursor(cursor, retval.pageSize, retval.total);
            } else {
                retval.nextPage = null;
            }
            return retval;
        }

    }

}
