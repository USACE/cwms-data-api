package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@JsonDeserialize(builder = TimeSeriesIdentifierDescriptors.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TimeSeriesIdentifierDescriptors extends CwmsDTOPaginated {

    private List<TimeSeriesIdentifierDescriptor> descriptors;

    private TimeSeriesIdentifierDescriptors() {

    }

    private int offset;

    private TimeSeriesIdentifierDescriptors(int offset, int pageSize, Integer total, List<TimeSeriesIdentifierDescriptor> specsList) {
        super(Integer.toString(offset), pageSize, total);
        descriptors = new ArrayList<>(specsList);
        this.offset = offset;
    }

    public List<TimeSeriesIdentifierDescriptor> getDescriptors() {
        return Collections.unmodifiableList(descriptors);
    }


    @Override
    public void validate() throws FieldException {

    }

    public static class Builder {
        private final int offset;
        private final int pageSize;
        private final Integer total;
        private List<TimeSeriesIdentifierDescriptor> descriptors;

        public Builder(int offset, int pageSize, Integer total) {
            this.offset = offset;
            this.pageSize = pageSize;
            this.total = total;
        }

        public Builder withDescriptors(Collection<TimeSeriesIdentifierDescriptor> descList) {
            this.descriptors = null;
            if(descList != null) {
                this.descriptors = new ArrayList<>(descList);
            }

            return this;
        }

        public TimeSeriesIdentifierDescriptors build() {
            TimeSeriesIdentifierDescriptors retval = new TimeSeriesIdentifierDescriptors(offset,
                    pageSize, total, descriptors);

            if (this.descriptors.size() == this.pageSize) {
                String cursor = Integer.toString(retval.offset + retval.descriptors.size());
                retval.nextPage = encodeCursor(cursor, retval.pageSize, retval.total);
            } else {
                retval.nextPage = null;
            }
            return retval;
        }

    }

}
