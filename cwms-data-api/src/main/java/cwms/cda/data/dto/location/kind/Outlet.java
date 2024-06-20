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
import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationIdentifier;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.XML})
@JsonDeserialize(builder = Outlet.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class Outlet implements CwmsDTOBase
{
	private final LocationIdentifier projectIdentifier;
	private final Location structureLocation;
	private final CharacteristicRef characteristicRef;

	private Outlet(Builder builder)
	{
		projectIdentifier = builder.projectIdentifier;
		structureLocation = builder.structureLocation;
		characteristicRef = builder.characteristicRef;
	}

	public CharacteristicRef getCharacteristicRef()
	{
		return characteristicRef;
	}

	public LocationIdentifier getProjectIdentifier()
	{
		return projectIdentifier;
	}

	public Location getStructureLocation()
	{
		return structureLocation;
	}

	@Override
	public String toString()
	{
		return "Outlet{" +
				"characteristicRef=" + characteristicRef +
				", projectIdentifier=" + projectIdentifier +
				", structureLocation=" + structureLocation +
				'}';
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
		Outlet outlet = (Outlet) o;
		return Objects.equals(getProjectIdentifier(), outlet.getProjectIdentifier()) && Objects.equals(getStructureLocation(), outlet.getStructureLocation()) && Objects.equals(getCharacteristicRef(), outlet.getCharacteristicRef());
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(getProjectIdentifier(), getStructureLocation(), getCharacteristicRef());
	}

	@Override
	public void validate() throws FieldException
	{
		List<String> missingFields = new ArrayList<>();
		if (projectIdentifier == null)
		{
			missingFields.add("projectIdentifier");
		}

		if (structureLocation == null)
		{
			missingFields.add("structureLocation");
		}

		if (!missingFields.isEmpty())
		{
			throw new RequiredFieldException(missingFields);
		}

		projectIdentifier.validate();
		structureLocation.validate();

		if (characteristicRef != null)
		{
			characteristicRef.validate();
		}
	}

	public static final class Builder
	{
		private LocationIdentifier projectIdentifier;
		private Location structureLocation;
		private CharacteristicRef characteristicRef;

		public Outlet build()
		{
			return new Outlet(this);
		}

		public Builder withCharacteristicRef(CharacteristicRef characteristicRef)
		{
			this.characteristicRef = characteristicRef;
			return this;
		}

		public Builder withProjectIdentifier(LocationIdentifier projectIdentifier)
		{
			this.projectIdentifier = projectIdentifier;
			return this;
		}

		public Builder withStructureLocation(Location structureLocation)
		{
			this.structureLocation = structureLocation;
			return this;
		}
	}
}
