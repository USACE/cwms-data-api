package fixtures;

public class TestAccounts {
    

    public enum KeyUser {
        SPK_NORMAL("l2hectest","l2userkey","notimplementedyet"),
        SPK_NO_ROLES("user2","User2key","notimplementedyet");

        private final String name;
        private final String apikey;
        private final String jSessionId;

        private KeyUser(String name, String key, String jSessionId) {
            this.name = name;
            this.apikey = key;
            this.jSessionId = jSessionId;
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
    }
}
