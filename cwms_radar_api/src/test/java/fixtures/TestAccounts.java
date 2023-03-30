package fixtures;

import java.util.Arrays;

public class TestAccounts {
    

    public enum KeyUser {
        GUEST("guest",null,null,null), // USED as marker label for tests
        SPK_NORMAL("l2hectest","1234567890","l2userkey","ATotallyRandomString","CWMS User", "cac_user"),
        SPK_NO_ROLES("user2",null,"User2key","notimplementedyet");

        private final String name;
        private final String edipi;
        private final String apikey;
        private final String jSessionId;
        private final String[] roles;

        private KeyUser(String name, String edipi, String key, String jSessionId, String... roles) {
            this.name = name;
            this.edipi = edipi;
            this.apikey = key;
            this.jSessionId = jSessionId;
            this.roles = roles;
        }

        public String toHeaderValue() {
            return String.format("apikey %s",apikey);
        }

        public String getJSessionId() {
            return jSessionId;
        }

        public String getEdipi() {
            return edipi;
        }

        public String getName() {
            return name;
        }

        public String getApikey() {
            return apikey;
        }

        public String[] getRoles() {
            return Arrays.copyOf(roles,roles.length);
        }
    }
}
