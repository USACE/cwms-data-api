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
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.XML})
@JsonDeserialize(builder = CharacteristicRef.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class CharacteristicRef implements CwmsDTOBase
{
	private final String officeId;
	private final String characteristicId;

	private CharacteristicRef(Builder builder)
	{
		officeId = builder.officeId;
		characteristicId = builder.characteristicId;
	}

	public String getCharacteristicId()
	{
		return characteristicId;
	}

	public String getOfficeId()
	{
		return officeId;
	}

	@Override
	public void validate() throws FieldException
	{
		List<String> missingFields = new ArrayList<>();
		if (officeId == null || officeId.isEmpty())
		{
			missingFields.add("officeId");
		}

		if (characteristicId == null || characteristicId.isEmpty())
		{
			missingFields.add("characteristicId");
		}

		if (!missingFields.isEmpty())
		{
			throw new RequiredFieldException(missingFields);
		}
	}

	@Override
	public String toString()
	{
		return "CharacteristicRef{" +
				"characteristicId='" + characteristicId + '\'' +
				", officeId='" + officeId + '\'' +
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
		CharacteristicRef that = (CharacteristicRef) o;
		return Objects.equals(getOfficeId(), that.getOfficeId()) && Objects.equals(getCharacteristicId(), that.getCharacteristicId());
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(getOfficeId(), getCharacteristicId());
	}

	public static final class Builder
	{
		private String officeId;
		private String characteristicId;

		public CharacteristicRef build()
		{
			return new CharacteristicRef(this);
		}

		public Builder withCharacteristicId(String characteristicId)
		{
			this.characteristicId = characteristicId;
			return this;
		}

		public Builder withOfficeId(String officeId)
		{
			this.officeId = officeId;
			return this;
		}
	}
}
