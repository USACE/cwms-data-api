package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
public class Blob extends CwmsDTO
{
	@JsonProperty(required=true)
	private String id;
	private String description;
	private String mediaTypeId;
	private byte[] value;

	private Blob() {
		super(null);
	}

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
        return getOfficeId() + "/" + id + ";description=" + description;
	}

	@Override
	protected void validateInternal(CwmsDTOValidator validator) {
		super.validateInternal(validator);
		validator.required(getOfficeId(), "office-id");
		validator.required(getId(), "id");
		validator.required(getValue(), "value");
	}
}
