package cwms.radar.data.dao;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.List;
import java.util.Optional;

import io.javalin.http.Context;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;

public abstract class JooqDao<T> extends Dao<T>
{

	protected JooqDao(DSLContext dsl)
	{
		super(dsl);
	}

	public static DSLContext getDslContext(Context ctx)
	{
		Connection database = (Connection) ctx.attribute("database");
		String officeId = ctx.attribute("office_id");
		return getDslContext(database, officeId);
	}

	public static DSLContext getDslContext(Connection database, String officeId)
	{
		DSLContext dsl =  DSL.using(database, SQLDialect.ORACLE11G);
		CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dsl.configuration(), officeId);

		return dsl;
	}

	@Override
	public List<T> getAll(Optional<String> limitToOffice)
	{
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public Optional<T> getByUniqueName(String uniqueName, Optional<String> limitToOffice)
	{
		throw new UnsupportedOperationException("Not supported yet.");
	}

	static Double toDouble(BigDecimal bigDecimal)
	{
		Double retval = null;
		if (bigDecimal != null)
		{
			retval = bigDecimal.doubleValue();
		}

		return retval;
	}
}
