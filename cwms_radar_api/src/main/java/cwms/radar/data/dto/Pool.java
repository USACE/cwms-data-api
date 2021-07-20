package cwms.radar.data.dto;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import usace.cwms.db.dao.ifc.pool.PoolNameType;
import usace.cwms.db.dao.ifc.pool.PoolType;

public class Pool extends PoolType implements CwmsDTO
{
	private Number attribute;
	private String description;

	private String clobText;

	public Pool(PoolNameType poolName, String projectId, String bottomLevelId, String topLevelId, boolean implicit)
	{
		super(poolName, projectId, bottomLevelId, topLevelId, implicit);
	}

	public Pool(PoolType p)
	{
		super(p.getPoolName(), p.getProjectId(), p.getBottomLevelId(), p.getTopLevelId(), p.isImplicit());
	}

	public Number getAttribute()
	{
		return attribute;
	}

	public void setAttribute(Number attribute)
	{
		this.attribute = attribute;
	}

	public String getDescription()
	{
		return description;
	}

	public void setDescription(String description)
	{
		this.description = description;
	}

	public String getClobText()
	{
		return clobText;
	}

	public void setClobText(String clobText)
	{
		this.clobText = clobText;
	}

	@Override
	public boolean equals(Object o)
	{
		if(this == o)
		{
			return true;
		}
		if(o == null || getClass() != o.getClass())
		{
			return false;
		}
		if(!super.equals(o))
		{
			return false;
		}

		final Pool pool = (Pool) o;

		if(getAttribute() != null ? !getAttribute().equals(pool.getAttribute()) : pool.getAttribute() != null)
		{
			return false;
		}
		if(getDescription() != null ? !getDescription().equals(pool.getDescription()) : pool.getDescription() != null)
		{
			return false;
		}

		return getClobText() != null ? getClobText().equals(pool.getClobText()) : pool.getClobText() == null;
	}

	@Override
	public int hashCode()
	{
		int result = super.hashCode();
		result = 31 * result + (getAttribute() != null ? getAttribute().hashCode() : 0);
		result = 31 * result + (getDescription() != null ? getDescription().hashCode() : 0);

		result = 31 * result + (getClobText() != null ? getClobText().hashCode() : 0);
		return result;
	}

	public static Pool fromString(String input){
		Pool retval= null;
		Pattern pattern = Pattern.compile("^(.*)/(.*):(.*)$");
		Matcher matcher = pattern.matcher(input);

		if(matcher.matches()){
			PoolNameType poolName = new PoolNameType(matcher.group(3), matcher.group(1) );
			retval = new Pool(poolName, matcher.group(2), null, null, true );
		}

		return retval;
	}

	// This is used in the Pools cursor.
	@Override
	public String toString(){
		StringBuilder builder = new StringBuilder();
		PoolNameType poolName = getPoolName();

		builder.append(poolName.getOfficeId())
				.append("/").append(getProjectId())
				.append(":").append(poolName.getPoolName());
		return builder.toString();
	}


}
