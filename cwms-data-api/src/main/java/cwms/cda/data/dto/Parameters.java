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

import java.util.ArrayList;
import java.util.List;

@JsonRootName("parameters")
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.XML})
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
public class Parameters extends CwmsDTO
{
	private List<Parameter> parameters;

	public Parameters()
	{
		super(null);
	}

	public Parameters(String officeId, List<Parameter> parameters)
	{
		super(officeId);
		this.parameters = new ArrayList<>(parameters);
	}

	public List<Parameter> getParameters()
	{
		return parameters;
	}

	@Override
	public void validate() throws FieldException
	{
		//Nothing to validate
	}
}
