package cwms.radar.data.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import cwms.radar.api.errors.FieldException;
import cwms.radar.api.errors.FieldsException;

@XmlRootElement(name="clob")
@XmlAccessorType(XmlAccessType.FIELD)
public class Clob implements CwmsDTO
{
	private String office;
	private String id;
	private String description;
	private String value;

	@SuppressWarnings("unused")
	private Clob(){}

	public Clob(String office, String id, String description, String value)
	{
		this.office = office;
		this.id = id;
		this.description = description;
		this.value = value;
	}

	public String getOffice()
	{
		return office;
	}

	public String getId()
	{
		return id;
	}

	public String getDescription()
	{
		return description;
	}

	public String getValue()
	{
		return value;
	}

	@Override
	public String toString(){
		StringBuilder builder = new StringBuilder();
		builder.append(getOffice()).append("/").append(id).append(";description=").append(description);
		return builder.toString();
	}

	@Override
	public void validate() throws FieldException {
		if( id.isEmpty() ){
			throw new FieldsException("clob id cannot be blank");
		}
		if( office.isEmpty() ){
			throw new FieldsException("office cannot be blank");
		}
	}
}
