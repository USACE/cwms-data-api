package cwms.radar.data.dao;

import static cwms.radar.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.flogger.FluentLogger;
import cwms.radar.api.errors.AlreadyExists;
import cwms.radar.data.dto.Clob;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
class ClobDaoTest {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String OFFICE = "SWT";

    @AfterAll
    static void tearDown() throws SQLException {
        try (DSLContext dsl = getDslContext(OFFICE)) {
            ClobDao dao = new ClobDao(dsl);

            List<Clob> found = dao.getClobsLike(OFFICE, "TEST/TEST%");
            logger.atFine().log("Found: " + found.size());

            for (Clob clob : found) {
                dao.delete(clob.getOffice(), clob.getId());
            }

            found = dao.getClobsLike(OFFICE, "TEST/TEST%");
            logger.atFine().log("After delete found: " + found.size());
        }
    }

    @Test
    void test_create() throws SQLException {

        long millis = System.currentTimeMillis();

        try (DSLContext dsl = getDslContext(OFFICE)) {
            ClobDao dao = new ClobDao(dsl);
            // FWIW the id field gets made into a capital ID in the database.
            String id = "TEST/TEST_ID_" + millis;
            Clob clob = new Clob(OFFICE, id, "test description", "test value");
            assertDoesNotThrow(()->dao.create(clob, true));
        }

    }

    @Test
    void test_create_already() throws SQLException {

        long millis = System.currentTimeMillis();
        // create once
        String id = "TEST/TEST_ID_" + millis;
        try (DSLContext dsl = getDslContext(OFFICE)) {
            ClobDao dao = new ClobDao(dsl);
            Clob clob = new Clob(OFFICE, id, "test description", "test value");
            dao.create(clob, true);
        }

        // try to create again and fail if it doesn't throw AlreadyExists
        try (DSLContext dsl = getDslContext(OFFICE)) {
            ClobDao dao = new ClobDao(dsl);

            Clob clob = new Clob(OFFICE, id, "test description", "test value");
            dao.create(clob, true);
            fail();
        } catch (AlreadyExists e) {
            // expected
        }


    }

    @Test
    void test_get() throws SQLException {

        String id = "TEST/TEST_ID_NOT_PRESENT";
        try (DSLContext dsl = getDslContext(OFFICE)) {
            ClobDao dao = new ClobDao(dsl);

            Optional<Clob> found = dao.getByUniqueName(id, Optional.of(OFFICE));
            assertFalse(found.isPresent());
        }

    }

    @Test
    void test_not_found_create_then_found() throws SQLException {

        long millis = System.currentTimeMillis();
        String id = "TEST/TEST_ID_" + millis;

        try (DSLContext dsl = getDslContext(OFFICE)) {
            ClobDao dao = new ClobDao(dsl);

            // try and fail to find it
            Optional<Clob> found = dao.getByUniqueName(id, Optional.of(OFFICE));
            assertFalse(found.isPresent());

            // create it
            Clob clob = new Clob(OFFICE, id, "test description", "test value");
            dao.create(clob, true);

            // try and find it
            found = dao.getByUniqueName(id, Optional.of(OFFICE));
            assertTrue(found.isPresent());
        }

    }

    @Test
    void test_not_found_create_found_delete_not_found() throws SQLException {

        long millis = System.currentTimeMillis();
        String id = "TEST/TEST_ID_" + millis;

        String office = OFFICE;
        try (DSLContext dsl = getDslContext(office)) {
            ClobDao dao = new ClobDao(dsl);

            // try and fail to find it
            Optional<Clob> found = dao.getByUniqueName(id, Optional.of(office));
            assertFalse(found.isPresent());

            // create it
            Clob clob = new Clob(office, id, "test description", "test value");
            dao.create(clob, true);

            // try and find it
            found = dao.getByUniqueName(id, Optional.of(office));
            assertTrue(found.isPresent());

            // delete it
            dao.delete(office, id);

            // try and fail to find it
            found = dao.getByUniqueName(id, Optional.of(office));
            assertFalse(found.isPresent());
        }

    }


    @Test
    void test_delete_doesnt_exist() throws SQLException {

        String id = "TEST/TEST_ID_NOT_PRESENT";
        try (DSLContext dsl = getDslContext(OFFICE)) {
            ClobDao dao = new ClobDao(dsl);


            assertDoesNotThrow(()->dao.delete(OFFICE, id));
            // This should be fine - no exceptions, no error.

        }

    }

    @Test
    void test_create_delete_delete() throws SQLException {

        long millis = System.currentTimeMillis();
        String id = "TEST/TEST_ID_" + millis;

        String office = OFFICE;
        try (DSLContext dsl = getDslContext(office)) {
            ClobDao dao = new ClobDao(dsl);

            // create it
            Clob clob = new Clob(office, id, "test description", "test value");
            dao.create(clob, true);

            assertDoesNotThrow(()->dao.delete(office, id));
            assertDoesNotThrow(()->dao.delete(office, id));
        }

    }

    @Test
    void test_create_update_find() throws SQLException {

        long millis = System.currentTimeMillis();
        String id = "TEST/TEST_ID_" + millis;

        String office = OFFICE;
        try (DSLContext dsl = getDslContext(office)) {
            ClobDao dao = new ClobDao(dsl);

            // create it
            Clob clob = new Clob(office, id, "test description", "test value");
            dao.create(clob, true);

            // update it
            clob = new Clob(office, id, "NEW description", "NEW value");

            dao.update(clob, false);

            // find it
            Optional<Clob> found = dao.getByUniqueName(id, Optional.of(office));
            assertTrue(found.isPresent());
            Clob foundClob = found.get();
            assertEquals("NEW description", foundClob.getDescription());
            assertEquals("NEW value", foundClob.getValue());

            dao.delete(office, id);
        }

    }

    @Test
    void test_create_update_find_nulls() throws SQLException {

        long millis = System.currentTimeMillis();
        String id = "TEST/TEST_ID_" + millis;

        String office = OFFICE;
        try (DSLContext dsl = getDslContext(office)) {
            ClobDao dao = new ClobDao(dsl);

            // create it
            String origDesc = "test description";
            Clob clob = new Clob(office, id, origDesc, "test value");
            dao.create(clob, true);

            // update it with null desc and a new Value
            String newValue = "NEW value";
            clob = new Clob(office, id, null, newValue);
            dao.update(clob, false);

            // find it
            Optional<Clob> found = dao.getByUniqueName(id, Optional.of(office));
            assertTrue(found.isPresent());
            Clob foundClob = found.get();
            assertNull(foundClob.getDescription());
            assertEquals(newValue, foundClob.getValue());

            try {
                // try to update it with null "value"
                String newDescription = "NEW Description";
                clob = new Clob(office, id, newDescription, null);
                dao.update(clob, false);
                fail();
            } catch (IllegalArgumentException e) {
                // expected
                String message = e.getMessage();
                assertTrue(message.contains("ORA-20244: NULL_ARGUMENT: Argument P_TEXT is not "
                        + "allowed to be null"));
            }

            // find it again
            found = dao.getByUniqueName(id, Optional.of(office));
            assertTrue(found.isPresent());
            foundClob = found.get();
            assertNull(foundClob.getDescription());
            assertEquals(newValue, foundClob.getValue());

            try {
                // update value with empty (are these treated same as null?)
                clob = new Clob(office, id, origDesc, "");
                dao.update(clob, false);
                fail();
            } catch (IllegalArgumentException e) {
                // expected
                String message = e.getMessage();
                assertTrue(message.contains("ORA-20244: NULL_ARGUMENT: Argument P_TEXT is not "
                        + "allowed to be null"));
            }

            found = dao.getByUniqueName(id, Optional.of(office));
            assertTrue(found.isPresent());
            foundClob = found.get();
            assertEquals(newValue, foundClob.getValue());
            assertNull(foundClob.getDescription());

            // update it with space
            clob = new Clob(office, id, " ", " ");
            dao.update(clob, false);

            found = dao.getByUniqueName(id, Optional.of(office));
            assertTrue(found.isPresent());
            foundClob = found.get();
            // empty is apparently treated the same as null, too bad.  Space is not.
            assertEquals(" ", foundClob.getValue());
            assertEquals(" ", foundClob.getDescription());

            dao.delete(office, id);
        }

    }


}