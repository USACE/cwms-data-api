package cwms.cda.data.dto.timeseriesprofile;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;

@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
@JsonDeserialize(builder = ParameterInfo.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class ParameterInfo implements CwmsDTOBase
{
	private final String parameter;
	private final String unit;
	private final int index;
	protected ParameterInfo(ParameterInfo.Builder builder)
	{
		parameter= builder.parameter;
		unit = builder.unit;
		index = builder.index;
	}
	public String getParameter()
	{
		return parameter;
	}
	public String getUnit()
	{
		return unit;
	}

	public int getIndex(){ return index; }


	@Override
	public void validate() throws FieldException
	{

	}
	@Override
	public int hashCode() {
		int result = Objects.hashCode(getParameter());
		result = 31 * result + Objects.hashCode(getUnit());
		result = 31 * result + Objects.hashCode(getIndex());
		return result;
	}@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ParameterInfo that = (ParameterInfo) o;
		return Objects.equals(getParameter(), that.getParameter())
				&& Objects.equals(getUnit(), that.getUnit())
				&& Objects.equals(getIndex(), that.getIndex());
	}
	@JsonPOJOBuilder
	@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
	public static final class Builder
	{
		private String parameter;
		private String unit;
		private int index;


		public ParameterInfo.Builder withParameter(String parameter) {
			this.parameter = parameter;
			return this;
		}
		public ParameterInfo.Builder withUnit(String unit) {
			this.unit = unit;
			return this;
		}
		public ParameterInfo.Builder withIndex(int index)
		{
			this.index = index;
			return this;
		}

		public ParameterInfo build() {
			return new ParameterInfo(this);
		}

	}
}