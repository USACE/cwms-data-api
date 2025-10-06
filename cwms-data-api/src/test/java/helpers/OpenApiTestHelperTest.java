package helpers;

import cwms.cda.api.OfficeController;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OpenApiTestHelperTest {

    @Test
    void test_helper_on_office() {
        List<Method> methods = OpenApiTestHelper.findByName(OfficeController.class, "getAll");
        assertEquals(1, methods.size());

        Method m = OpenApiTestHelper.findOneByName(OfficeController.class, "getAll");

        assertNotNull(m);
        OpenApiTestHelper.OpenApiDocInfo info = OpenApiTestHelper.readDocParams(m);
        assertNotNull(info);
        assertEquals(2, info.query.size());
        assertTrue(info.query.contains("format"));
        assertTrue(info.query.contains("has-data"));

        assertEquals(0, info.path.size());

    }

    @Test
    void test_helper_on_office_bad_name() {
        try{
            OpenApiTestHelper.findOneByName(OfficeController.class, "bad");
            fail("Should have thrown exception");
        } catch(RuntimeException ex){
            assertEquals("Did not find method with name bad", ex.getMessage());
        }
    }

    @Test
    void test_find_handlers() {
        List<String> classes = OpenApiTestHelper.findCrudHandlerClasses();
        assertNotNull(classes);
        assertTrue(classes.size() > 100);
        assertTrue(classes.contains("cwms.cda.api.OfficeController"));
        assertTrue(classes.contains("cwms.cda.api.TimeSeriesController"));
        assertTrue(classes.contains("cwms.cda.api.rating.RatingController"));  // CrudHandler in sub package
        assertTrue(classes.contains("cwms.cda.api.TextTimeSeriesValueController"));  // Handler
        assertTrue(classes.contains("cwms.cda.api.watersupply.WaterUserDeleteController")); // Handler in sub-package
        assertTrue(classes.contains("cwms.cda.api.auth.users.roles.AddRoleController")); // Handler in sub-sub-package

        // fail if any class starts with io.javalin
        classes.forEach(c -> assertFalse(c.startsWith("io.javalin")));
    }



}