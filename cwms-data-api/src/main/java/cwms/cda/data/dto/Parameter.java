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
	private String name;
	private String baseParameter;
	private String subParameter;
	private String subParameterDescription;
	private String dbUnitId;
	private String unitLongName;
	private String unitDescription;

	public Parameter()
	{
		super(null);
	}

	public Parameter(String name, String baseParameter, String subParameter, String subParameterDescription, String dbOfficeId, String dbUnitId, String unitLongName, String unitDescription)
	{
		super(dbOfficeId);
		this.name = name;
		this.baseParameter = baseParameter;
		this.subParameter = subParameter;
		this.subParameterDescription = subParameterDescription;
		this.dbUnitId = dbUnitId;
		this.unitLongName = unitLongName;
		this.unitDescription = unitDescription;
	}

	public String getBaseParameter()
	{
		return baseParameter;
	}

	public String getDbUnitId()
	{
		return dbUnitId;
	}

	public String getName()
	{
		return name;
	}

	public String getSubParameter()
	{
		return subParameter;
	}

	public String getSubParameterDescription()
	{
		return subParameterDescription;
	}

	public String getUnitDescription()
	{
		return unitDescription;
	}

	public String getUnitLongName()
	{
		return unitLongName;
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
		return Objects.equals(getName(), parameter.getName()) && Objects.equals(getBaseParameter(), parameter.getBaseParameter()) && Objects.equals(getSubParameter(), parameter.getSubParameter()) && Objects.equals(getSubParameterDescription(), parameter.getSubParameterDescription()) && Objects.equals(getDbUnitId(), parameter.getDbUnitId()) && Objects.equals(getUnitLongName(), parameter.getUnitLongName()) && Objects.equals(getUnitDescription(), parameter.getUnitDescription());
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(getName(), getBaseParameter(), getSubParameter(), getSubParameterDescription(), getDbUnitId(), getUnitLongName(), getUnitDescription());
	}
}
