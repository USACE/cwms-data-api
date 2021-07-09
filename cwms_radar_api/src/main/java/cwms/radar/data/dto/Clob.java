package cwms.radar.data.dto;

public class Clob implements CwmsDTO
{
	private String office;
	private String id;
	private String description;
	private String value;

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
}
