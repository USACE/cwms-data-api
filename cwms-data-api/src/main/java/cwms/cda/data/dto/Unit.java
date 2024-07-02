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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonRootName("unit")
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.XML})
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class Unit extends CwmsDTO
{
	private String abstractParameter;
	private String name;
	private String longName;
	private String unitSystem;
	private String description;
	private List<String> alternateNames;

	public Unit(String name, String longName, String abstractParameter, String description, String unitSystem, List<String> alternateNames)
	{
		super(null);
		this.abstractParameter = abstractParameter;
		this.name = name;
		this.longName = longName;
		this.unitSystem = unitSystem;
		this.description = description;
		this.alternateNames = new ArrayList<>(alternateNames);
	}

	public String getDescription()
	{
		return description;
	}

	public Unit()
	{
		super(null);
	}

	public String getLongName()
	{
		return longName;
	}

	public String getName()
	{
		return name;
	}

	public String getUnitSystem()
	{
		return unitSystem;
	}

	public List<String> getAlternateNames()
	{
		return new ArrayList<>(alternateNames);
	}

	public String getAbstractParameter()
	{
		return abstractParameter;
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
		Unit unit = (Unit) o;
		return Objects.equals(getName(), unit.getName()) && Objects.equals(getLongName(), unit.getLongName()) && Objects.equals(getUnitSystem(), unit.getUnitSystem()) && Objects.equals(getDescription(), unit.getDescription()) && Objects.equals(getAlternateNames(), unit.getAlternateNames());
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(getName(), getLongName(), getUnitSystem(), getDescription(), getAlternateNames());
	}
}
