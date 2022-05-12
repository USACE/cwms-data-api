package cwms.radar.data.dto.rating;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.radar.api.errors.FieldException;
import cwms.radar.data.dto.CwmsDTO;
import org.jetbrains.annotations.NotNull;

import hec.data.Version;
import hec.data.rating.IRatingTemplate;

@JsonDeserialize(builder = RatingTemplate.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class RatingTemplate implements CwmsDTO
{
	private final String officeId;
	private final String id;
	private String  version;

	private String description;
	private String dependentParameter;

	private List<ParameterSpec> independentParameterSpecs;

	private List<String> specs;
	public RatingTemplate(Builder builder)
	{
		this.id = builder.id;
		this.version = builder.version;
		this.officeId = builder.officeId;
		this.description = builder.description;
		this.dependentParameter = builder.dependentParameter;
		this.independentParameterSpecs = builder.independentParameterList;
		this.specs = builder.specs;
	}

	public String getId()
	{
		return id;
	}

	public String getVersion()
	{
		return version;
	}

	public String getOfficeId()
	{
		return officeId;
	}

	public String getDescription()
	{
		return description;
	}

	public String getDependentParameter()
	{
		return dependentParameter;
	}

	public List<ParameterSpec> getIndependentParameterSpecs()
	{
		return independentParameterSpecs;
	}



	public List<String> getSpecs()
	{
		return specs;
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

		final RatingTemplate that = (RatingTemplate) o;

		if(getOfficeId() != null ? !getOfficeId().equals(that.getOfficeId()) : that.getOfficeId() != null)
		{
			return false;
		}
		if(getId() != null ? !getId().equals(that.getId()) : that.getId() != null)
		{
			return false;
		}
		if(getVersion() != null ? !getVersion().equals(that.getVersion()) : that.getVersion() != null)
		{
			return false;
		}
		if(getDependentParameter() != null ? !getDependentParameter().equals(
				that.getDependentParameter()) : that.getDependentParameter() != null)
		{
			return false;
		}
		return getIndependentParameterSpecs() != null ? getIndependentParameterSpecs().equals(
				that.getIndependentParameterSpecs()) : that.getIndependentParameterSpecs() == null;
	}

	@Override
	public int hashCode()
	{
		int result = getOfficeId() != null ? getOfficeId().hashCode() : 0;
		result = 31 * result + (getId() != null ? getId().hashCode() : 0);
		result = 31 * result + (getVersion() != null ? getVersion().hashCode() : 0);
		result = 31 * result + (getDependentParameter() != null ? getDependentParameter().hashCode() : 0);
		result = 31 * result + (getIndependentParameterSpecs() != null ? getIndependentParameterSpecs().hashCode() : 0);
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

		private List<String> specs;

		public Builder withVersion(String  templateVersion) {
			this.version = templateVersion;
			return this;
		}

		public Builder withOfficeId(String officeId) {
			this.officeId = officeId;
			return this;
		}

		public Builder withId(String id){
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

		public Builder withIndependentParameters(List<ParameterSpec> independentParameterList) {
			this.independentParameterList = independentParameterList;
			return this;
		}



//		public Builder withIndependentParameterList(List<? extends mil.army.usace.hec.metadata.Parameter> independentParameterList) {
//			this.independentParameterList = independentParameterList.stream().map(Parameter::getParameter).collect(
//					Collectors.toList());
//
//			return this;
//		}

		static String getVersion(IRatingTemplate template) {
			String retval = null;

			if(template != null) {
				Version ttv = template.getTemplateVersion();
				if (ttv != null) {
					mil.army.usace.hec.metadata.Version metaData = ttv.getVersionMetaData();
					retval = metaData.getVersion();
				}
			}

			return  retval;
		}

		public Builder fromTemplate(IRatingTemplate template) {
			Builder retval = this;
			retval = retval.withId(template.toString());
			retval = retval.withVersion(getVersion(template));
			retval = retval.withOfficeId(template.getOfficeId().getOfficeId());
			retval = retval.withDependentParameter(template.getDependentParameter());

			List<ParameterSpec> independentParameterSpecs = buildParameterSpecs(template.getIndependentParameterList());
			retval = retval.withIndependentParameters(independentParameterSpecs);

			return retval;
		}

		@NotNull
		private List<ParameterSpec> buildParameterSpecs(List<hec.data.Parameter> independentParameterList1)
		{
			List<ParameterSpec> independentParameterSpecs = new ArrayList<>();
			if(independentParameterList1 != null) {
				for(hec.data.Parameter independentParameter : independentParameterList1) {
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
			retval = retval.withIndependentParameters(template.getIndependentParameterSpecs());
			retval = retval.withSpecs(template.getSpecs());

			return retval;
		}

		public Builder withSpecs(List<String> specs) {
			if(specs != null) {
				this.specs = new ArrayList<>(specs);
			} else {
				this.specs = null;
			}
			return this;
		}

		public RatingTemplate build () {
			return new RatingTemplate(this);
		}
	}



}
