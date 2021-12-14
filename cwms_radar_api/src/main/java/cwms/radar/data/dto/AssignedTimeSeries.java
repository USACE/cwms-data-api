package cwms.radar.data.dto;

import java.math.BigDecimal;

public class AssignedTimeSeries implements CwmsDTO
{
	private String timeseriesId;
	private BigDecimal tsCode;

	private String aliasId;
	private String refTsId;
	private Integer attribute;


	public AssignedTimeSeries(String timeseriesId, BigDecimal tsCode, String aliasId, String refTsId, Integer attr)
	{
		this.timeseriesId = timeseriesId;
		this.tsCode = tsCode;
		this.aliasId = aliasId;
		this.refTsId = refTsId;
		this.attribute = attr;
	}

	public String getTimeseriesId()
	{
		return timeseriesId;
	}

	public BigDecimal getTsCode()
	{
		return tsCode;
	}

	public String getAliasId()
	{
		return aliasId;
	}

	public String getRefTsId()
	{
		return refTsId;
	}

	public Integer getAttribute()
	{
		return attribute;
	}
}
