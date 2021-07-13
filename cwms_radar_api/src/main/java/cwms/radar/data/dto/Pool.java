package cwms.radar.data.dto;

import usace.cwms.db.dao.ifc.pool.PoolNameType;
import usace.cwms.db.dao.ifc.pool.PoolType;

public class Pool extends PoolType implements CwmsDTO
{
	public Pool(PoolNameType poolName, String projectId, String bottomLevelId, String topLevelId, boolean implicit)
	{
		super(poolName, projectId, bottomLevelId, topLevelId, implicit);
	}

	public Pool(PoolType p)
	{
		super(p.getPoolName(), p.getProjectId(), p.getBottomLevelId(), p.getTopLevelId(), p.isImplicit());
	}

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
