package cwms.radar.data.dto;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import usace.cwms.db.dao.ifc.pool.PoolNameType;
import usace.cwms.db.dao.ifc.pool.PoolType;

public class Pool extends PoolType implements CwmsDTO
{
	private final Number attribute;
	private final String description;
	private final String clobText;

	public Pool(Builder b){
		super(b.getPoolName(), b.getProjectId(), b.getBottomLevelId(), b.getTopLevelId(), b.isImplicit());
		this.attribute = b.getAttribute();
		this.description = b.getDescription();
		this.clobText = b.getClobText();
	}

	public Number getAttribute()
	{
		return attribute;
	}


	public String getDescription()
	{
		return description;
	}

	public String getClobText()
	{
		return clobText;
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
			Builder builder = Builder.newInstance();
			builder.withPoolName(poolName);
			builder.withProjectId(matcher.group(2));
			builder.withBottomLevelId(null);
			builder.withTopLevelId(null);
			builder.withImplicit(true);
			retval = builder.build();
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

	public static class Builder
	{
		private PoolNameType poolName;

		private String projectId;
		private String bottomLevelId;
		private String topLevelId;
		private boolean isImplicit;
		private Number attribute;
		private String description;
		private String clobText;


		public PoolNameType getPoolName()
		{
			return poolName;
		}

		public Builder withPoolName(PoolNameType poolName)
		{
			this.poolName = poolName;
			return this;
		}

		public String getProjectId()
		{
			return projectId;
		}

		public Builder withProjectId(String projectId)
		{
			this.projectId = projectId;
			return this;
		}

		public String getBottomLevelId()
		{
			return bottomLevelId;
		}

		public Builder withBottomLevelId(String bottomLevelId)
		{
			this.bottomLevelId = bottomLevelId;
			return this;

		}

		public String getTopLevelId()
		{
			return topLevelId;
		}

		public Builder withTopLevelId(String topLevelId)
		{
			this.topLevelId = topLevelId;
			return this;
		}

		public boolean isImplicit()
		{
			return isImplicit;
		}

		public Builder withImplicit(boolean implicit)
		{
			isImplicit = implicit;
			return this;
		}

		public Number getAttribute()
		{
			return attribute;
		}

		public Builder withAttribute(Number attribute)
		{
			this.attribute = attribute;
			return this;
		}

		public String getDescription()
		{
			return description;
		}

		public Builder withDescription(String description)
		{
			this.description = description;
			return this;
		}

		public String getClobText()
		{
			return clobText;
		}

		public Builder withClobText(String clobText)
		{
			this.clobText = clobText;
			return this;
		}

		public static Builder newInstance()
		{
			return new Builder();
		}

		public Pool build()
		{
			return new Pool(this);
		}

		public Builder withPoolType(PoolType poolType)
		{
			withPoolName(poolType.getPoolName());
			withBottomLevelId(poolType.getBottomLevelId());
			withTopLevelId(poolType.getTopLevelId());
			withProjectId(poolType.getProjectId());
			withImplicit(poolType.isImplicit());

			return this;
		}
	}


}
