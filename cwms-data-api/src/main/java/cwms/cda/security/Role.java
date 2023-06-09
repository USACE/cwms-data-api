package cwms.cda.security;

import io.javalin.core.security.RouteRole;

public class Role implements RouteRole {
    String name;

    public Role(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Role namedRole = (Role) o;

        return name != null ? name.equals(namedRole.name) : namedRole.name == null;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Role{" + "name='" + name + '\'' + '}';
    }
}
