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

import java.util.List;

@JsonRootName("time-zones")
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.XML})
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class TimeZones extends CwmsDTO
{
	private List<TimeZone> timeZones;

	@SuppressWarnings("unused") // for JAXB to handle marshalling
	private TimeZones()
	{
		super(null);
	}

	public TimeZones(List<TimeZone> timeZones)
	{
		super(null);
		this.timeZones = timeZones;
	}

	public List<TimeZone> getTimeZones()
	{
		return timeZones;
	}

	@Override
	public void validate() throws FieldException
	{
		//No validation needed
	}
}
