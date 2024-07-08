package fixtures.users;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import cwms.auth.CwmsUserPrincipal;

public class MockCwmsUserPrincipalImpl extends CwmsUserPrincipal {
    private String name;
    private String edipi;
    private List<String> roles = new ArrayList<>();

    public MockCwmsUserPrincipalImpl(String name, String edipi, String... roles) {
        this.name = name;
        this.edipi = edipi;
        for (String role: roles) {
            this.roles.add(role);
        }
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getEdipi() {
        return this.edipi;
    }

    @Override
    public String getSessionKey() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getSessionKey'");
    }

    @Override
    public String getSessionUUID() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getSessionUUID'");
    }

    @Override
    public ZonedDateTime getLastLogin() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getLastLogin'");
    }

    @Override
    public List<String> getRoles() {
       return this.roles;
    }

}