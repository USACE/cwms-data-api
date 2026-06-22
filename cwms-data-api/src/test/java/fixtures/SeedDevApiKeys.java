/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dto.auth.ApiKey;
import cwms.cda.security.CwmsAuthException;
import cwms.cda.security.DataApiPrincipal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

public final class SeedDevApiKeys {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final String KEY_NAME_PREFIX = "dev-gradle-seed-";

    private SeedDevApiKeys() {
    }

    public static void main(String[] args) throws Exception {
        String jdbcDriver = requiredProperty("CDA_JDBC_DRIVER");
        String jdbcUrl = requiredProperty("CDA_JDBC_URL");
        String jdbcUsername = requiredProperty("CDA_JDBC_USERNAME");
        String jdbcPassword = requiredProperty("CDA_JDBC_PASSWORD");
        String defaultOffice = System.getProperty("CDA_OFFICE", "HQ");
        String keyName = KEY_NAME_PREFIX + UUID.randomUUID();

        Class.forName(jdbcDriver);

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("user", jdbcUsername);
        connectionProperties.setProperty("password", jdbcPassword);

        LOGGER.atInfo().log(
              "\n============================================================\n"
                    + "Creating development API keys\n"
                    + "============================================================\n"
                    + "JDBC URL: %s\n"
                    + "Key name: %s",
              jdbcUrl,
              keyName
        );

        try (Connection connection = DriverManager.getConnection(jdbcUrl, connectionProperties)) {
            DSLContext dsl = DSL.using(connection, SQLDialect.DEFAULT);
            List<String> users = getHecTestUsers(dsl);
            if (users.isEmpty()) {
                LOGGER.atWarning().log("No **HECTEST users in the database. No keys were created.");
            }
            AuthDao authDao = AuthDao.getInstance(dsl, defaultOffice);

            for (String user : users) {
                createAndPrintKey(authDao, user, keyName);
            }
        }
        LOGGER.atInfo().log(
              "\n============================================================\n"
                    + "Development API key seeding complete.\n"
                    + "These plaintext keys are shown only because this dev seeder created them.\n"
                    + "============================================================"
        );
    }

    private static List<String> getHecTestUsers(DSLContext dsl) {
        var userId = DSL.field(DSL.name("USERID"), String.class);
        return dsl.select(userId)
              .from(DSL.table(DSL.name("CWMS_20", "AT_SEC_CWMS_USERS")))
              .where(userId.like("%HECTEST"))
              .fetch(userId);
    }

    private static void createAndPrintKey(AuthDao authDao, String user, String keyName) throws CwmsAuthException {
        DataApiPrincipal principal = new DataApiPrincipal(user, Collections.emptySet());
        ApiKey sourceData = new ApiKey(
              user,
              keyName,
              null,
              null,
              null
        );

        ApiKey createdKey = authDao.createApiKey(principal, sourceData);

        LOGGER.atInfo().log(
              "\n------------------------------------------------------------"
                    + "Created development API key:\nUser: %s\nAPI key name: %s\nAPI key: %s",
              createdKey.getUserId(),
              createdKey.getKeyName(),
              createdKey.getApiKey()
        );
    }

    private static String requiredProperty(String propertyName) {
        String value = System.getProperty(propertyName);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required system property: " + propertyName);
        }
        return value;
    }
}