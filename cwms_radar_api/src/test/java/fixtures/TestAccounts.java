package fixtures;

import java.util.Arrays;

public class TestAccounts {
    

    public enum KeyUser {
        GUEST("guest",null,null,null,null), // USED as marker label for tests
        SPK_NORMAL("l2hectest","l2hectest","1234567890","l2userkey","ATotallyRandomString","CWMS User", "cac_user"),
        SPK_NO_ROLES("user2","user2",null,"User2key","user2SEssion");

        private final String name; // username
        private final String edipi; // CAC # value
        private final String apikey; // 
        private final String jSessionId;
        private final String password; // used for Keycloak login to get JWT
        private final String[] roles;

        private KeyUser(String name, String password, String edipi, String key, String jSessionId, String... roles) {
            this.name = name;
            this.edipi = edipi;
            this.password = password;
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

        public String getPassword() {
            return password;
        }

        public String getApikey() {
            return apikey;
        }

        public String[] getRoles() {
            return Arrays.copyOf(roles,roles.length);
        }
    }
}
