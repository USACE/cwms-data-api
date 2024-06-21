/*
 * Copyright (c) 2024. Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved. HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval from HEC
 */

package cwms.cda.data.dto.location.kind;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationIdentifier;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = Outlet.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class Outlet extends ProjectStructure
{
	private final CharacteristicRef characteristicRef;

	private Outlet(Builder builder)
	{
		super(builder.projectId, builder.location);
		characteristicRef = builder.characteristicRef;
	}

	public CharacteristicRef getCharacteristicRef()
	{
		return characteristicRef;
	}

	@Override
	public void validate() throws FieldException
	{
		//No opped
	}

	public static final class Builder
	{
		private CharacteristicRef characteristicRef;
		private LocationIdentifier projectId;
		private Location location;

		public Outlet build()
		{
			return new Outlet(this);
		}

		public Builder withCharacteristicRef(CharacteristicRef characteristicRef)
		{
			this.characteristicRef = characteristicRef;
			return this;
		}

		public Builder withProjectId(LocationIdentifier projectIdentifier)
		{
			this.projectId = projectIdentifier;
			return this;
		}

		public Builder withLocation(Location location)
		{
			this.location = location;
			return this;
		}
	}
}
