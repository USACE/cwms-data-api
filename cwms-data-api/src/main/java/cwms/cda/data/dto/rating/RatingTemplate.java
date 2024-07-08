package cwms.cda.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import hec.data.Version;
import hec.data.rating.IRatingTemplate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.jetbrains.annotations.NotNull;

@JsonDeserialize(builder = RatingTemplate.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
public class RatingTemplate extends CwmsDTO {
    private final String id;
    private final String version;

    private final String description;
    private final String dependentParameter;

    private final List<ParameterSpec> independentParameterSpecs;

    private final List<String> ratingIds;

    public RatingTemplate(Builder builder) {
        super(builder.officeId);
        this.id = builder.id;
        this.version = builder.version;
        this.description = builder.description;
        this.dependentParameter = builder.dependentParameter;
        this.independentParameterSpecs = builder.independentParameterList;
        this.ratingIds = builder.ratingIds;
    }

    public String getId() {
        return id;
    }

    public String getVersion() {
        return version;
    }

    public String getDescription() {
        return description;
    }

    public String getDependentParameter() {
        return dependentParameter;
    }

    public List<ParameterSpec> getIndependentParameterSpecs() {
        return independentParameterSpecs;
    }


    public List<String> getRatingIds() {
        return ratingIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final RatingTemplate that = (RatingTemplate) o;

        if (getOfficeId() != null ? !getOfficeId().equals(that.getOfficeId()) :
				that.getOfficeId() != null) {
            return false;
        }
        if (getId() != null ? !getId().equals(that.getId()) : that.getId() != null) {
            return false;
        }
        if (getVersion() != null ? !getVersion().equals(that.getVersion()) :
				that.getVersion() != null) {
            return false;
        }
        if (getDependentParameter() != null ? !getDependentParameter().equals(
                that.getDependentParameter()) : that.getDependentParameter() != null) {
            return false;
        }
        return getIndependentParameterSpecs() != null ? getIndependentParameterSpecs().equals(
                that.getIndependentParameterSpecs()) : that.getIndependentParameterSpecs() == null;
    }

    @Override
    public int hashCode() {
        int result = getOfficeId() != null ? getOfficeId().hashCode() : 0;
        result = 31 * result + (getId() != null ? getId().hashCode() : 0);
        result = 31 * result + (getVersion() != null ? getVersion().hashCode() : 0);
        result = 31 * result + (getDependentParameter() != null
				? getDependentParameter().hashCode() : 0);
        result = 31 * result + (getIndependentParameterSpecs() != null
				? getIndependentParameterSpecs().hashCode() : 0);
        return result;
    }

    @Override
    public void validate() throws FieldException {
        // TODO Auto-generated method stub

    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private String id;
        private String version;
        private String officeId;
        private String description;
        private String dependentParameter;
        private List<ParameterSpec> independentParameterList;

        private List<String> ratingIds;

        public Builder withVersion(String templateVersion) {
            this.version = templateVersion;
            return this;
        }

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

		public Builder withDependentParameter(mil.army.usace.hec.metadata.Parameter dependentParameter) {
            this.dependentParameter = dependentParameter.getParameter();
            return this;
        }

        public Builder withDependentParameter(String dependentParameter) {
            this.dependentParameter = dependentParameter;
            return this;
        }

        public Builder withIndependentParameterSpecs(List<ParameterSpec> independentParameterList) {
            this.independentParameterList = independentParameterList;
            return this;
        }

        static String getVersion(IRatingTemplate template) {
            String retval = null;

            if (template != null) {
                Version ttv = template.getTemplateVersion();
                if (ttv != null) {
                    mil.army.usace.hec.metadata.Version metaData = ttv.getVersionMetaData();
                    retval = metaData.getVersion();
                }
            }

            return retval;
        }

        public Builder fromTemplate(IRatingTemplate template) {
            Builder retval = this;
            retval = retval.withId(template.toString());
            retval = retval.withVersion(getVersion(template));
            retval = retval.withOfficeId(template.getOfficeId().getOfficeId());
            retval = retval.withDependentParameter(template.getDependentParameter());

            List<ParameterSpec> independentParameterSpecs =
					buildParameterSpecs(template.getIndependentParameterList());
            retval = retval.withIndependentParameterSpecs(independentParameterSpecs);

            return retval;
        }

        @NotNull
        private List<ParameterSpec> buildParameterSpecs(List<hec.data.Parameter> independentParameterList1) {
            List<ParameterSpec> independentParameterSpecs = new ArrayList<>();
            if (independentParameterList1 != null) {
                for (hec.data.Parameter independentParameter : independentParameterList1) {
                    independentParameterSpecs.add(new ParameterSpec(independentParameter.getParameter()));
                }
            }
            return independentParameterSpecs;
        }

        public Builder fromRatingTemplate(RatingTemplate template) {
            Builder retval = this;
            retval = retval.withId(template.getId());
            retval = retval.withVersion(template.getVersion());
            retval = retval.withOfficeId(template.getOfficeId());
            retval = retval.withDescription(template.getDescription());
            retval = retval.withDependentParameter(template.getDependentParameter());
            retval = retval.withIndependentParameterSpecs(template.getIndependentParameterSpecs());
            retval = retval.withRatingIds(template.getRatingIds());

            return retval;
        }

        public Builder withRatingIds(Collection<String> specs) {
            if (specs != null) {
                this.ratingIds = new ArrayList<>(specs);
            } else {
                this.ratingIds = null;
            }
            return this;
        }

        public RatingTemplate build() {
            return new RatingTemplate(this);
        }
    }


}
