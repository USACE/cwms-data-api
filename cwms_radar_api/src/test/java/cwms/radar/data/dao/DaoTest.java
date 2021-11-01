package cwms.radar.data.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DaoTest
{

	public static Connection getConnection() throws SQLException
	{
		// See:  https://stackoverflow.com/a/32761503/249623
		// For one way this can be done for Intellij.
		// Or you can locally edit this file and hard
		// code your values here and just not submit this file.
		String url = System.getenv("RADAR_JDBC_URL" );
		String user = System.getenv("RADAR_JDBC_USERNAME" );
		String password = System.getenv("RADAR_JDBC_PASSWORD");

		assertNotNull(url, "RADAR_JDBC_URL should have been set in the Environment Variables");
		assertNotNull(user, "RADAR_JDBC_USERNAME should have been set in the Environment Variables");
		assertNotNull(password, "RADAR_JDBC_PASSWORD should have been set in the Environment Variables");

		Properties connectionProps = new Properties();
		connectionProps.put("user", user);
		connectionProps.put("password", password);
		final Connection conn = DriverManager.getConnection(url, connectionProps);
		return conn;
	}

	public static DSLContext getDslContext(Connection database, String officeId)
	{
		DSLContext dsl =  DSL.using(database, SQLDialect.ORACLE11G);
		CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dsl.configuration(), officeId);
		return dsl;
	}

}
