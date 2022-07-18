package cwms.radar.data.dto.rating;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.radar.api.errors.FieldException;
import cwms.radar.data.dto.CwmsDTO;

@JsonDeserialize(builder = RatingSpec.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class RatingSpec implements CwmsDTO
{
	private final String officeId;
	private final String ratingId;
	private final String templateId;
	private final String locationId;
	private final String version;
	private final String sourceAgency;

	private final String inRangeMethod;
	private final String outRangeLowMethod;
	private final String outRangeHighMethod;
	private final boolean active;
	private final boolean autoUpdate;
	private final boolean autoActivate;
	private final boolean autoMigrateExtension;
	private final IndependentRoundingSpec[] independentRoundingSpecs;
	private final String dependentRoundingSpec;
	private final String description;

	@JsonFormat(shape = JsonFormat.Shape.STRING)
	private List<ZonedDateTime> effectiveDates;


	public RatingSpec(Builder builder){
		this.officeId = builder.officeId;
		this.ratingId = builder.ratingId;
		this.templateId = builder.templateId;
		this.locationId = builder.locationId;
		this.version = builder.version;
		this.sourceAgency = builder.sourceAgency;
		this.inRangeMethod = builder.inRangeMethod;
		this.outRangeLowMethod = builder.outRangeLowMethod;
		this.outRangeHighMethod = builder.outRangeHighMethod;
		this.active = builder.active;
		this.autoUpdate = builder.autoUpdate;
		this.autoActivate = builder.autoActivate;
		this.autoMigrateExtension = builder.autoMigrateExtension;
		this.independentRoundingSpecs = builder.independentRoundingSpecs;
		this.dependentRoundingSpec = builder.dependentRoundingSpec;
		this.description = builder.description;
		this.effectiveDates = builder.effectiveDates;
	}

	public String getOfficeId()
	{
		return officeId;
	}

	public String getRatingId()
	{
		return ratingId;
	}

	public String getTemplateId()
	{
		return templateId;
	}

	public String getLocationId()
	{
		return locationId;
	}

	public String getVersion()
	{
		return version;
	}

	public String getSourceAgency()
	{
		return sourceAgency;
	}

	public String getInRangeMethod()
	{
		return inRangeMethod;
	}

	public String getOutRangeLowMethod()
	{
		return outRangeLowMethod;
	}

	public String getOutRangeHighMethod()
	{
		return outRangeHighMethod;
	}

	public boolean isActive()
	{
		return active;
	}

	public boolean isAutoUpdate()
	{
		return autoUpdate;
	}

	public boolean isAutoActivate()
	{
		return autoActivate;
	}

	public boolean isAutoMigrateExtension()
	{
		return autoMigrateExtension;
	}

	public IndependentRoundingSpec[] getIndependentRoundingSpecs()
	{
		return independentRoundingSpecs;
	}

	public String getDependentRoundingSpec()
	{
		return dependentRoundingSpec;
	}

	public String getDescription()
	{
		return description;
	}

	public List<ZonedDateTime> getEffectiveDates() {
		return effectiveDates;
	}

	@Override
	public void validate() throws FieldException
	{

	}

	public static class IndependentRoundingSpec
	{
		private final Integer position;

		private final String value;

		public IndependentRoundingSpec(@JsonProperty("position") Integer position, @JsonProperty("value") String value){
			this.position = position;
			this.value = value;
		}

		public IndependentRoundingSpec(String value){
			this.position = null;
			this.value = value;
		}

		public Integer getPosition()
		{
			return position;
		}

		public String getValue()
		{
			return value;
		}

		@Override
		public boolean equals(Object o)
		{
			if(this == o)
			{
				return true;
			}
			if(o == null || getClass() != o.getClass())
			{
				return false;
			}

			final IndependentRoundingSpec that = (IndependentRoundingSpec) o;

			return getValue() != null ? getValue().equals(that.getValue()) : that.getValue() == null;
		}

		@Override
		public int hashCode()
		{
			return getValue() != null ? getValue().hashCode() : 0;
		}
	}

	@Override
	public boolean equals(Object o)
	{
		if(this == o)
		{
			return true;
		}
		if(o == null || getClass() != o.getClass())
		{
			return false;
		}

		final RatingSpec that = (RatingSpec) o;

		if(isActive() != that.isActive())
		{
			return false;
		}
		if(isAutoUpdate() != that.isAutoUpdate())
		{
			return false;
		}
		if(isAutoActivate() != that.isAutoActivate())
		{
			return false;
		}
		if(isAutoMigrateExtension() != that.isAutoMigrateExtension())
		{
			return false;
		}
		if(getOfficeId() != null ? !getOfficeId().equals(that.getOfficeId()) : that.getOfficeId() != null)
		{
			return false;
		}
		if(getRatingId() != null ? !getRatingId().equals(
				that.getRatingId()) : that.getRatingId() != null)
		{
			return false;
		}
		if(getTemplateId() != null ? !getTemplateId().equals(that.getTemplateId()) : that.getTemplateId() != null)
		{
			return false;
		}
		if(getLocationId() != null ? !getLocationId().equals(that.getLocationId()) : that.getLocationId() != null)
		{
			return false;
		}
		if(getVersion() != null ? !getVersion().equals(that.getVersion()) : that.getVersion() != null)
		{
			return false;
		}
		if(getSourceAgency() != null ? !getSourceAgency().equals(
				that.getSourceAgency()) : that.getSourceAgency() != null)
		{
			return false;
		}
		// Probably incorrect - comparing Object[] arrays with Arrays.equals
		if(!Arrays.equals(getIndependentRoundingSpecs(), that.getIndependentRoundingSpecs()))
		{
			return false;
		}
		if(getDependentRoundingSpec() != null ? !getDependentRoundingSpec().equals(
				that.getDependentRoundingSpec()) : that.getDependentRoundingSpec() != null)
		{
			return false;
		}
		return getDescription() != null ? getDescription().equals(
				that.getDescription()) : that.getDescription() == null;
	}

	@Override
	public int hashCode()
	{
		int result = getOfficeId() != null ? getOfficeId().hashCode() : 0;
		result = 31 * result + (getRatingId() != null ? getRatingId().hashCode() : 0);
		result = 31 * result + (getTemplateId() != null ? getTemplateId().hashCode() : 0);
		result = 31 * result + (getLocationId() != null ? getLocationId().hashCode() : 0);
		result = 31 * result + (getVersion() != null ? getVersion().hashCode() : 0);
		result = 31 * result + (getSourceAgency() != null ? getSourceAgency().hashCode() : 0);
		result = 31 * result + (isActive() ? 1 : 0);
		result = 31 * result + (isAutoUpdate() ? 1 : 0);
		result = 31 * result + (isAutoActivate() ? 1 : 0);
		result = 31 * result + (isAutoMigrateExtension() ? 1 : 0);
		result = 31 * result + Arrays.hashCode(getIndependentRoundingSpecs());
		result = 31 * result + (getDependentRoundingSpec() != null ? getDependentRoundingSpec().hashCode() : 0);
		result = 31 * result + (getDescription() != null ? getDescription().hashCode() : 0);
		return result;
	}

	@JsonPOJOBuilder
	@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
	public static class Builder {

		private String officeId;
		private String ratingId;
		private String templateId;
		private String locationId;
		private String version;
		private String sourceAgency;
		private String inRangeMethod;
		private String outRangeLowMethod;
		private String outRangeHighMethod;
		private boolean active;
		private boolean autoUpdate;
		private boolean autoActivate;
		private boolean autoMigrateExtension;
		private IndependentRoundingSpec[] independentRoundingSpecs;
		private String dependentRoundingSpec;
		private String description;

		private List<ZonedDateTime> effectiveDates;


		public Builder withOfficeId(String officeId) {
			this.officeId = officeId;
			return this;
		}

		public Builder withRatingId(String ratingSpecId) {
			this.ratingId = ratingSpecId;
			return this;
		}

		public Builder withTemplateId(String templateId) {
			this.templateId = templateId;
			return this;
		}

		public Builder withLocationId(String locationId) {
			this.locationId = locationId;
			return this;
		}

		public Builder withVersion(String version) {
			this.version = version;
			return this;
		}


		public Builder withSourceAgency(String sourceAgency) {
			this.sourceAgency = sourceAgency;
			return this;
		}

		public Builder withInRangeMethod(String inRangeMethod) {
			this.inRangeMethod = inRangeMethod;
			return this;
		}


		public Builder withOutRangeLowMethod(String outRangeLowMethod) {
			this.outRangeLowMethod = outRangeLowMethod;
			return this;
		}

		public Builder withOutRangeHighMethod(String outRangeHighMethod) {
			this.outRangeHighMethod = outRangeHighMethod;
			return this;
		}

		public Builder withActive(boolean active) {
			this.active = active;
			return this;
		}

		public Builder withAutoUpdate(boolean autoUpdate) {
			this.autoUpdate = autoUpdate;
			return this;
		}

		public Builder withAutoActivate(boolean autoActivate) {
			this.autoActivate = autoActivate;
			return this;
		}

		public Builder withAutoMigrateExtension(boolean autoMigrateExtension) {
			this.autoMigrateExtension = autoMigrateExtension;
			return this;
		}

		public Builder withIndependentRoundingSpecs(IndependentRoundingSpec[] indRoundingSpecs) {
			this.independentRoundingSpecs = indRoundingSpecs;
			return this;
		}

		public static IndependentRoundingSpec[] buildIndependentRoundingSpecs(String indRoundingSpecsStr)
		{
			IndependentRoundingSpec[] retval = null;
			if(indRoundingSpecsStr != null && !indRoundingSpecsStr.isEmpty()) {
				String[] indRoundingSpecsStrArr = indRoundingSpecsStr.split("/");
				retval = new IndependentRoundingSpec[indRoundingSpecsStrArr.length];
				for(int i = 0; i < indRoundingSpecsStrArr.length; i++) {
					retval[i] = new IndependentRoundingSpec(indRoundingSpecsStrArr[i]);
				}
			}
			return retval;
		}

		public Builder withDependentRoundingSpec(String depRoundingSpec) {
			this.dependentRoundingSpec = depRoundingSpec;
			return this;
		}

		public Builder withDescription(String description) {
			this.description = description;
			return this;
		}
		public Builder withEffectiveDates(List<ZonedDateTime> dates)
		{
			if(dates != null && !dates.isEmpty()) {
				this.effectiveDates = new ArrayList<>(dates);
			} else {
				this.effectiveDates = null;
			}

			return this;
		}

		public RatingSpec build () {
			return new RatingSpec(this);
		}

		public Builder fromRatingSpec(RatingSpec spec)
		{
			withOfficeId(spec.getOfficeId());
			withRatingId(spec.getRatingId());
			withTemplateId(spec.getTemplateId());
			withLocationId(spec.getLocationId());
			withVersion(spec.getVersion());
			withSourceAgency(spec.getSourceAgency());
			withInRangeMethod(spec.getInRangeMethod());
			withOutRangeLowMethod(spec.getOutRangeLowMethod());
			withOutRangeHighMethod(spec.getOutRangeHighMethod());
			withActive(spec.isActive());
			withAutoUpdate(spec.isAutoUpdate());
			withAutoActivate(spec.isAutoActivate());
			withAutoMigrateExtension(spec.isAutoMigrateExtension());
			withIndependentRoundingSpecs(spec.getIndependentRoundingSpecs());
			withDependentRoundingSpec(spec.getDependentRoundingSpec());
			withDescription(spec.getDescription());
			withEffectiveDates(spec.getEffectiveDates());

			return this;
		}

        public Builder withDateMethods(String dateMethods) {
            if (dateMethods != null && !dateMethods.isEmpty()) {
                String[] parts = dateMethods.split(",");
                if (parts.length > 0) {
                    withOutRangeLowMethod(parts[0]);
                }

                if (parts.length > 1) {
                    withInRangeMethod(parts[1]);
                }

                if (parts.length > 2) {
                    withOutRangeHighMethod(parts[2]);
                }
            } else {
                withInRangeMethod(null);
                withOutRangeLowMethod(null);
                withOutRangeHighMethod(null);
            }
            return this;
        }
	}
}
