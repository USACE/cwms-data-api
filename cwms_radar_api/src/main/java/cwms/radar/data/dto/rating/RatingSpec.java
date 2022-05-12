package cwms.radar.data.dto.rating;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

@JsonDeserialize(builder = RatingSpec.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class RatingSpec
{
	private final String officeId;
	private final String ratingSpecId;
	private final String templateId;
	private final String locationId;
	private final String version;
	private final String sourceAgency;
//	private final String inRangeMethod;
//	private final String outRangeLowMethod;
//	private final String outRangeHighMethod;
	private final boolean active;
	private final boolean autoUpdate;
	private final boolean autoActivate;
	private final boolean autoMigrateExtension;
	private final IndRoundingSpec[] indRoundingSpecs;
	private final String depRoundingSpec;
	private final String description;

	@JsonFormat(shape = JsonFormat.Shape.STRING)
	private List<ZonedDateTime> effectiveDates;



	public RatingSpec(Builder builder){
		this.officeId = builder.officeId;
		this.ratingSpecId = builder.ratingSpecId;
		this.templateId = builder.templateId;
		this.locationId = builder.locationId;
		this.version = builder.version;
		this.sourceAgency = builder.sourceAgency;
//		this.inRangeMethod = builder.inRangeMethod;
//		this.outRangeLowMethod = builder.outRangeLowMethod;
//		this.outRangeHighMethod = builder.outRangeHighMethod;
		this.active = builder.active;
		this.autoUpdate = builder.autoUpdate;
		this.autoActivate = builder.autoActivate;
		this.autoMigrateExtension = builder.autoMigrateExtension;
		this.indRoundingSpecs = builder.indRoundingSpecs;
		this.depRoundingSpec = builder.depRoundingSpec;
		this.description = builder.description;
		this.effectiveDates = builder.effectiveDates;
	}

	public String getOfficeId()
	{
		return officeId;
	}

	public String getRatingSpecId()
	{
		return ratingSpecId;
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

//	public String getInRangeMethod()
//	{
//		return inRangeMethod;
//	}
//
//	public String getOutRangeLowMethod()
//	{
//		return outRangeLowMethod;
//	}
//
//	public String getOutRangeHighMethod()
//	{
//		return outRangeHighMethod;
//	}

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

	public IndRoundingSpec[] getIndRoundingSpecs()
	{
		return indRoundingSpecs;
	}

	public String getDepRoundingSpec()
	{
		return depRoundingSpec;
	}

	public String getDescription()
	{
		return description;
	}

	public List<ZonedDateTime> getEffectiveDates() {
		return effectiveDates;
	}

	public static class IndRoundingSpec {
		private final Integer position;
		private final String value;

		public IndRoundingSpec(Integer position, String value){
			this.position = position;
			this.value = value;
		}

		public IndRoundingSpec(String value){
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

			final IndRoundingSpec that = (IndRoundingSpec) o;

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
		if(getRatingSpecId() != null ? !getRatingSpecId().equals(
				that.getRatingSpecId()) : that.getRatingSpecId() != null)
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
		if(!Arrays.equals(getIndRoundingSpecs(), that.getIndRoundingSpecs()))
		{
			return false;
		}
		if(getDepRoundingSpec() != null ? !getDepRoundingSpec().equals(
				that.getDepRoundingSpec()) : that.getDepRoundingSpec() != null)
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
		result = 31 * result + (getRatingSpecId() != null ? getRatingSpecId().hashCode() : 0);
		result = 31 * result + (getTemplateId() != null ? getTemplateId().hashCode() : 0);
		result = 31 * result + (getLocationId() != null ? getLocationId().hashCode() : 0);
		result = 31 * result + (getVersion() != null ? getVersion().hashCode() : 0);
		result = 31 * result + (getSourceAgency() != null ? getSourceAgency().hashCode() : 0);
		result = 31 * result + (isActive() ? 1 : 0);
		result = 31 * result + (isAutoUpdate() ? 1 : 0);
		result = 31 * result + (isAutoActivate() ? 1 : 0);
		result = 31 * result + (isAutoMigrateExtension() ? 1 : 0);
		result = 31 * result + Arrays.hashCode(getIndRoundingSpecs());
		result = 31 * result + (getDepRoundingSpec() != null ? getDepRoundingSpec().hashCode() : 0);
		result = 31 * result + (getDescription() != null ? getDescription().hashCode() : 0);
		return result;
	}

	@JsonPOJOBuilder
	@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
	public static class Builder {

		private String officeId;
		private String ratingSpecId;
		private String templateId;
		private String locationId;
		private String version;
		private String sourceAgency;
//		private String inRangeMethod;
//		private String outRangeLowMethod;
//		private String outRangeHighMethod;
		private boolean active;
		private boolean autoUpdate;
		private boolean autoActivate;
		private boolean autoMigrateExtension;
		private IndRoundingSpec[] indRoundingSpecs;
		private String depRoundingSpec;
		private String description;

		private List<ZonedDateTime> effectiveDates;


		public Builder officeId(String officeId) {
			this.officeId = officeId;
			return this;
		}

		public Builder ratingSpecId(String ratingSpecId) {
			this.ratingSpecId = ratingSpecId;
			return this;
		}

		public Builder templateId(String templateId) {
			this.templateId = templateId;
			return this;
		}

		public Builder locationId(String locationId) {
			this.locationId = locationId;
			return this;
		}

		public Builder version(String version) {
			this.version = version;
			return this;
		}

		public Builder sourceAgency(String sourceAgency) {
			this.sourceAgency = sourceAgency;
			return this;
		}

//		public Builder inRangeMethod(String inRangeMethod) {
//			this.inRangeMethod = inRangeMethod;
//			return this;
//		}
//
//		public Builder outRangeLowMethod(String outRangeLowMethod) {
//			this.outRangeLowMethod = outRangeLowMethod;
//			return this;
//		}
//
//		public Builder outRangeHighMethod(String outRangeHighMethod) {
//			this.outRangeHighMethod = outRangeHighMethod;
//			return this;
//		}

		public Builder active(boolean active) {
			this.active = active;
			return this;
		}

		public Builder autoUpdate(boolean autoUpdate) {
			this.autoUpdate = autoUpdate;
			return this;
		}

		public Builder autoActivate(boolean autoActivate) {
			this.autoActivate = autoActivate;
			return this;
		}

		public Builder autoMigrateExtension(boolean autoMigrateExtension) {
			this.autoMigrateExtension = autoMigrateExtension;
			return this;
		}

		public Builder indRoundingSpecs(IndRoundingSpec[] indRoundingSpecs) {
			this.indRoundingSpecs = indRoundingSpecs;
			return this;
		}

		public Builder indRoundingSpecs(String indRoundingSpecsStr) {
			indRoundingSpecs(buildIndRoundingSpecs(indRoundingSpecsStr));

			return this;
		}

		private static IndRoundingSpec[] buildIndRoundingSpecs(String indRoundingSpecsStr)
		{
			IndRoundingSpec[] retval = null;
			if(indRoundingSpecsStr != null && !indRoundingSpecsStr.isEmpty()) {
				String[] indRoundingSpecsStrArr = indRoundingSpecsStr.split("/");
				retval = new IndRoundingSpec[indRoundingSpecsStrArr.length];
				for(int i = 0; i < indRoundingSpecsStrArr.length; i++) {
					retval[i] = new IndRoundingSpec(indRoundingSpecsStrArr[i]);
				}
			}
			return retval;
		}

		public Builder depRoundingSpec(String depRoundingSpec) {
			this.depRoundingSpec = depRoundingSpec;
			return this;
		}

		public Builder description(String description) {
			this.description = description;
			return this;
		}
		public Builder effectiveDates(List<ZonedDateTime> dates)
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
			officeId(spec.getOfficeId());
			ratingSpecId(spec.getRatingSpecId());
			templateId(spec.getTemplateId());
			locationId(spec.getLocationId());
			version(spec.getVersion());
			sourceAgency(spec.getSourceAgency());
//			inRangeMethod(spec.getInRangeMethod());
//			outRangeLowMethod(spec.getOutRangeLowMethod());
//			outRangeHighMethod(spec.getOutRangeHighMethod());
			active(spec.isActive());
			autoUpdate(spec.isAutoUpdate());
			autoActivate(spec.isAutoActivate());
			autoMigrateExtension(spec.isAutoMigrateExtension());
			indRoundingSpecs(spec.getIndRoundingSpecs());
			depRoundingSpec(spec.getDepRoundingSpec());
			description(spec.getDescription());
			effectiveDates(spec.getEffectiveDates());

			return this;
		}

	}
}
