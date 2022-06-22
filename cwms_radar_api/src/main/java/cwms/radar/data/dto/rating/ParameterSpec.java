package cwms.radar.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;


@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class ParameterSpec
{
	private String parameter;
	private String inRangeMethod;
	private String outRangeLowMethod;
	private String outRangeHighMethod;

	public ParameterSpec()
	{

	}

	public ParameterSpec(String parameter){
		this(parameter, null, null,null);

	}

	public ParameterSpec(String parameter, String inRangeMethod, String outOfRangeLowMethod, String outOfRangeHighMethod){
		this.parameter = parameter;
		this.inRangeMethod = inRangeMethod;
		this.outRangeLowMethod = outOfRangeLowMethod;
		this.outRangeHighMethod = outOfRangeHighMethod;

	}
	public String getParameter()
	{
		return parameter;
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

		final ParameterSpec that = (ParameterSpec) o;

		if(getParameter() != null ? !getParameter().equals(that.getParameter()) : that.getParameter() != null)
		{
			return false;
		}
		if(getInRangeMethod() != null ? !getInRangeMethod().equals(
				that.getInRangeMethod()) : that.getInRangeMethod() != null)
		{
			return false;
		}
		if(getOutRangeLowMethod() != null ? !getOutRangeLowMethod().equals(
				that.getOutRangeLowMethod()) : that.getOutRangeLowMethod() != null)
		{
			return false;
		}
		return getOutRangeHighMethod() != null ? getOutRangeHighMethod().equals(
				that.getOutRangeHighMethod()) : that.getOutRangeHighMethod() == null;
	}

	@Override
	public int hashCode()
	{
		int result = getParameter() != null ? getParameter().hashCode() : 0;
		result = 31 * result + (getInRangeMethod() != null ? getInRangeMethod().hashCode() : 0);
		result = 31 * result + (getOutRangeLowMethod() != null ? getOutRangeLowMethod().hashCode() : 0);
		result = 31 * result + (getOutRangeHighMethod() != null ? getOutRangeHighMethod().hashCode() : 0);
		return result;
	}
}
