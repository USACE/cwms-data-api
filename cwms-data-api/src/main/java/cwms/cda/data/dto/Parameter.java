/*
 * Copyright (c) 2024. Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved. HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval from HEC
 */

package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;

import java.util.Objects;

@JsonRootName("parameter")
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.XML})
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
public final class Parameter extends CwmsDTO
{
	private String param;
	private String baseParam;
	private String subParam;
	private String subParamDesc;
	private String dbUnitId;
	private String unitLongName;
	private String unitDesc;

	public Parameter()
	{
		super(null);
	}

	public Parameter(String param, String baseParam, String subParam, String subParamDesc, String dbOfficeId, String dbUnitId, String unitLongName, String unitDesc)
	{
		super(dbOfficeId);
		this.param = param;
		this.baseParam = baseParam;
		this.subParam = subParam;
		this.subParamDesc = subParamDesc;
		this.dbUnitId = dbUnitId;
		this.unitLongName = unitLongName;
		this.unitDesc = unitDesc;
	}

	public String getBaseParam()
	{
		return baseParam;
	}

	public String getDbUnitId()
	{
		return dbUnitId;
	}

	public String getParam()
	{
		return param;
	}

	public String getSubParam()
	{
		return subParam;
	}

	public String getSubParamDesc()
	{
		return subParamDesc;
	}

	public String getUnitDesc()
	{
		return unitDesc;
	}

	public String getUnitLongName()
	{
		return unitLongName;
	}

	@Override
	public void validate() throws FieldException
	{
		//No validation
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
		{
			return true;
		}
		if (o == null || getClass() != o.getClass())
		{
			return false;
		}
		Parameter parameter = (Parameter) o;
		return Objects.equals(getParam(), parameter.getParam()) && Objects.equals(getBaseParam(), parameter.getBaseParam()) && Objects.equals(getSubParam(), parameter.getSubParam()) && Objects.equals(getSubParamDesc(), parameter.getSubParamDesc()) && Objects.equals(getDbUnitId(), parameter.getDbUnitId()) && Objects.equals(getUnitLongName(), parameter.getUnitLongName()) && Objects.equals(getUnitDesc(), parameter.getUnitDesc());
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(getParam(), getBaseParam(), getSubParam(), getSubParamDesc(), getDbUnitId(), getUnitLongName(), getUnitDesc());
	}
}
