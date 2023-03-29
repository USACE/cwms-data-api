package fixtures;

import java.util.Arrays;

public class TestAccounts {
    

    public enum KeyUser {
        GUEST("guest",null,null), // USED as marker label for tests
        SPK_NORMAL("l2hectest","l2userkey","notimplementedyet","CWMS User", "cac_user"),
        SPK_NO_ROLES("user2","User2key","notimplementedyet");

        private final String name;
        private final String apikey;
        private final String jSessionId;
        private final String[] roles;

        private KeyUser(String name, String key, String jSessionId, String... roles) {
            this.name = name;
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
