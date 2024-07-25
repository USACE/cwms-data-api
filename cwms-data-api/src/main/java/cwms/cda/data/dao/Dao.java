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

package cwms.cda.data.dao;

import static usace.cwms.db.jooq.codegen.tables.AV_DB_CHANGE_LOG.AV_DB_CHANGE_LOG;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import cwms.cda.data.dto.CwmsDTO;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.ifc.env.CwmsDbEnv;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;

public abstract class Dao<T> {
    public static final int CWMS_18_1_8 = 180108;
    public static final int CWMS_21_1_1 = 210101;
    public static final int CWMS_23_03_16 = 230316;

    public static final String PROP_BASE = "cwms.cda.data.dao.dao";
    public static final String VERSION_NAME = "version";
    private static final Cache<String, Integer> versionCache = CacheBuilder.newBuilder()
            .maximumSize(Integer.getInteger(PROP_BASE + "." + VERSION_NAME
                    + ".maxSize", 8))
            .expireAfterWrite(Integer.getInteger(PROP_BASE + "." + VERSION_NAME
                    + ".expireAfterSeconds", 300), TimeUnit.SECONDS)
            .build();

    @SuppressWarnings("unused")
    protected DSLContext dsl;

    protected Dao(DSLContext dsl) {
        this.dsl = dsl;
    }

    private static String getVersion(DSLContext dsl) {
        return dsl.connectionResult(c -> DSL.using(c,SQLDialect.ORACLE18C)
                .select(AV_DB_CHANGE_LOG.VERSION)
                .from(AV_DB_CHANGE_LOG)
                .orderBy(AV_DB_CHANGE_LOG.VERSION_DATE.desc())
                .limit(1)
                .fetchOne().component1());
    }

    public int getDbVersion() {
        Integer cachedValue = versionCache.getIfPresent(VERSION_NAME);
        if (cachedValue == null) {
            int newValue = versionAsInteger(getVersion(dsl));
            versionCache.put(VERSION_NAME, newValue);
            return newValue;
        } else {
            return cachedValue;
        }
    }

    private static int versionAsInteger(String version) {
        String[] parts = version.split("\\.");

        return Integer.parseInt(parts[0]) * 10000
                + Integer.parseInt(parts[1]) * 100
                + Integer.parseInt(parts[2]);
    }


    /**
     * Sets session office on specific connection.
     * @param c opened connection
     * @param object Data containing a valid CWMS office
     * @throws SQLException if the underlying database throws an exception
     */
    protected void setOffice(Connection c, CwmsDTO object) throws SQLException {
        this.setOffice(c,object.getOfficeId());
    }

    protected void setOffice(Connection c, String office) throws SQLException {
        CwmsDbEnv db = CwmsDbServiceLookup.buildCwmsDb(CwmsDbEnv.class, c);
        db.setSessionOfficeId(c,office);
    }


    public abstract List<T> getAll(String office);

    public abstract Optional<T> getByUniqueName(String uniqueName, String office);

}
