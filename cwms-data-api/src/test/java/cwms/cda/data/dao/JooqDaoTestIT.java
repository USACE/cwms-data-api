package cwms.cda.data.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.datasource.SessionOfficePreparer;
import fixtures.CwmsDataApiSetupCallback;
import java.sql.Connection;
import java.sql.SQLException;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public final class JooqDaoTestIT extends DataApiTestIT {

    private static final String SESSION_OFFICE_QUERY =
            "SELECT SYS_CONTEXT('CWMS_ENV','SESSION_OFFICE_ID') FROM dual";

    /**
     * Verifies that SessionOfficePreparer sets the Oracle session variable
     * on a cold connection.  This is the preparer added to the chain in
     * JooqDao.getDslContext — without it, cold pooled connections fail
     * intermittently with ORA-20047 when CWMS PL/SQL checks session office.
     *
     * Uses a raw JDBC connection (not the API pool) to guarantee cold state.
     */
    @Test
    void sessionOfficePreparer_setsOfficeOnColdConnection() throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        String officeId = db.getOfficeId();
        String webUser = CwmsDataApiSetupCallback.getWebUser();

        db.connection(c -> {
            assertNull(readSessionOffice(c),
                    "Precondition: fresh connection should have no session office");

            new SessionOfficePreparer(officeId).prepare(c);

            assertEquals(officeId.toUpperCase(), readSessionOffice(c).toUpperCase(),
                    "SessionOfficePreparer should set SYS_CONTEXT session office");
        }, webUser);
    }

    /**
     * Verifies that SessionOfficePreparer correctly overrides a stale session
     * office from a previous request — the cross-office contamination case.
     * Without the preparer in the checkout chain, a pooled connection warmed
     * by one office retains that office for subsequent requests.
     */
    @Test
    void sessionOfficePreparer_overridesStaleOffice() throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        String officeId = db.getOfficeId();
        String webUser = CwmsDataApiSetupCallback.getWebUser();

        db.connection(c -> {
            new SessionOfficePreparer(officeId).prepare(c);
            assertEquals(officeId.toUpperCase(), readSessionOffice(c).toUpperCase(),
                    "Session should be " + officeId + " after first prepare");

            new SessionOfficePreparer("SPK").prepare(c);
            assertEquals("SPK", readSessionOffice(c).toUpperCase(),
                    "Session should switch to SPK — stale " + officeId + " must be overridden");
        }, webUser);
    }

    private static String readSessionOffice(Connection c) {
        return DSL.using(c, SQLDialect.ORACLE18C)
                .fetchOne(SESSION_OFFICE_QUERY)
                .get(0, String.class);
    }
}
