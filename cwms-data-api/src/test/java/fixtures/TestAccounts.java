/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package fixtures;

import java.util.Arrays;

public class TestAccounts {


    public enum KeyUser {
        NONE("none",null,null,null,null, null), // Used for annotations
        GUEST("guest",null,null,null, null, null), // USED as marker label for tests
        SPK_NORMAL("l2hectest","l2hectest","1234567890","l2userkey","ATotallyRandomStringL2hectest","SPK", "CWMS Users", "cac_user"),
        SPK_NORMAL2("l2hectest_vt","l2hectestvt","2345678901","l2userkey2","DiffrntStringL2hectest_vt","SPK", "CWMS Users", "cac_user"),
        SWT_NORMAL("m5hectest","swt99db","1234567890","testkey2","ATotallyRandomStringM5hectest","SWT", "CWMS Users", "cac_user"),
        SPK_NO_ROLES("user2","user2",null,"User2key","user2SEssion", "SPK");

        private final String name; // username
        private final String edipi; // CAC # value
        private final String apikey; //
        private final String jSessionId;
        private final String password; // used for Keycloak login to get JWT
        /**
         * Primary operating office for this user. Tests may use other offices and assign more privs as needed.
         */
        private final String operatingOffice;
        private final String[] roles;

        private KeyUser(String name, String password, String edipi, String key, String jSessionId, String operatingOffice, String... roles) {
            this.name = name;
            this.edipi = edipi;
            this.password = password;
            this.apikey = key;
            this.jSessionId = jSessionId;
            this.operatingOffice = operatingOffice;
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

        public String getOperatingOffice() {
            return operatingOffice;
        }

        public String[] getRoles() {
            return Arrays.copyOf(roles,roles.length);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("User{");
            sb.append("UserName=").append(this.name).append(",")
              .append("EDIPI=").append(this.edipi).append(",")
              .append("Key=").append(this.apikey).append(",")
              .append("JESSIONID=").append(this.jSessionId).append(",")
              .append("Password=").append(this.password).append(",");
            sb.append("Roles[").append(String.join(",",roles)).append("}");
            return sb.toString();
        }
    }
}
