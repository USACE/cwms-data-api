package cwms.radar.data.dao;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;
import org.apache.tomcat.jdbc.pool.PoolConfiguration;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import org.jooq.impl.DefaultConnectionProvider;
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


	public static DSLContext getDslContext(String officeId) throws SQLException {

		PoolConfiguration poolProperties = new org.apache.tomcat.jdbc.pool.PoolProperties();
		poolProperties.setUrl(System.getenv("RADAR_JDBC_URL"));
		poolProperties.setUsername(System.getenv("RADAR_JDBC_USERNAME"));
		poolProperties.setPassword(System.getenv("RADAR_JDBC_PASSWORD"));

		Driver driver = DriverManager.getDriver(System.getenv("RADAR_JDBC_URL"));
		poolProperties.setDriverClassName(driver.getClass().getName());
		DataSource ds = new org.apache.tomcat.jdbc.pool.DataSource(poolProperties);

		ConnectionProvider cp = new OfficeSettingConnectionProvider(ds, officeId);
		DSLContext dsl =  DSL.using(cp, SQLDialect.ORACLE11G);
		return dsl;
	}


}
