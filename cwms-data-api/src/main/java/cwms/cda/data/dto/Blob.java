package cwms.cda.data.dto;

import org.checkerframework.checker.units.qual.s;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import cwms.cda.api.errors.FieldException;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class Blob extends CwmsDTO
{
	@JsonProperty(required=true)
	private String id;
	private String description;
	private String mediaTypeId;
	private byte[] value;

	public Blob(String office, String id, String description, String type, byte[] value)
	{
		super(office);
		this.id = id;
		this.description = description;
		this.mediaTypeId = type;
		this.value = value;
	}

	public String getId()
	{
		return id;
	}

	public String getDescription()
	{
		return description;
	}

	public byte[] getValue()
	{
		return value;
	}

	public String getMediaTypeId()
	{
		return mediaTypeId;
	}

	@Override
	public String toString(){
		StringBuilder builder = new StringBuilder();
		builder.append(getOfficeId()).append("/").append(id).append(";description=").append(description);
		return builder.toString();
	}

	@Override
	public void validate() throws FieldException {
	}
}
