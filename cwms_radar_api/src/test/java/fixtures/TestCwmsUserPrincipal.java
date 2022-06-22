package fixtures;

import java.time.ZonedDateTime;
import java.util.List;

import cwms.auth.CwmsUserPrincipal;

public class TestCwmsUserPrincipal extends CwmsUserPrincipal
{

	private String name;
	private String sessionKey;
	private List<String> roles;
	private String edipi = null;
	private String sessionUUID = null;
	private ZonedDateTime lastLogin = null;

	public TestCwmsUserPrincipal(String name, String sessionKey, List<String> roles)
	{
		this.name = name;
		this.sessionKey = sessionKey;
		this.roles = roles;
	}

	public TestCwmsUserPrincipal(String name, String sessionKey, List<String> roles, String edipi,  String sessionUUID,
	                             ZonedDateTime lastLogin)
	{
		this(name, sessionKey, roles);

		this.edipi = edipi;
		this.sessionUUID = sessionUUID;
		this.lastLogin = lastLogin;
	}

	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public String getEdipi()
	{
		return edipi;
	}

	@Override
	public String getSessionKey()
	{
		return sessionKey;
	}

	@Override
	public String getSessionUUID()
	{
		return sessionUUID;
	}

	@Override
	public ZonedDateTime getLastLogin()
	{
		return lastLogin;
	}

	@Override
	public List<String> getRoles()
	{
		return roles;
	}
}
