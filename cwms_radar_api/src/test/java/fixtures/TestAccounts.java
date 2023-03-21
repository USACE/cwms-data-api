package fixtures;

public class TestAccounts {
    

    public enum KeyUser {
        SPK_NORMAL("l2hectest","l2userkey"),
        SPK_NO_ROLES("user2","User2key");

        private final String name;
        private final String key;

        private KeyUser(String name, String key) {
            this.name = name;
            this.key = key;
        }

        public String toHeaderValue() {
            return String.format("apikey %s",key);
        }

        public String getName() {
            return name;
        }

        public String getKey() {
            return key;
        }
    }
}
