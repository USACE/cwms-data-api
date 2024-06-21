/*
 * Copyright (c) 2024. Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved. HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval from HEC
 */

package cwms.cda.data.dto.location.kind;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationIdentifier;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public abstract class ProjectStructure implements CwmsDTOBase
{
	private final LocationIdentifier projectId;
	private final Location location;

	protected ProjectStructure(LocationIdentifier projectId, Location location)
	{
		this.location = location;
		this.projectId = projectId;
	}

	public final LocationIdentifier getProjectId()
	{
		return projectId;
	}

	public final Location getLocation()
	{
		return location;
	}

	@Override
	public void validate() throws FieldException
	{
		if (getLocation() == null) {
			throw new FieldException("Location field can't be null");
		}
		getLocation().validate();
		if (getProjectId() == null) {
			throw new FieldException("Project location Id field must be defined");
		}
		getProjectId().validate();
	}
}
