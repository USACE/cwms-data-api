package helpers;

import cwms.cda.api.OfficeController;
import cwms.cda.api.TextTimeSeriesValueController;
import cwms.cda.api.TimeSeriesController;
import cwms.cda.api.auth.users.roles.AddRoleController;
import cwms.cda.api.rating.RatingController;
import cwms.cda.api.watersupply.WaterUserDeleteController;
import io.javalin.apibuilder.CrudHandler;
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
        List<Class<CrudHandler>> classes = OpenApiTestHelper.findClassesOfType(CrudHandler.class, "cwms.cda.api");
        assertNotNull(classes);
        assertTrue(classes.contains(OfficeController.class));
        assertTrue(classes.contains(TimeSeriesController.class));
        assertTrue(classes.contains(RatingController.class));  // CrudHandler in sub package
        assertTrue(classes.contains(TextTimeSeriesValueController.class));  // Handler
        assertTrue(classes.contains(WaterUserDeleteController.class)); // Handler in sub-package
        assertTrue(classes.contains(AddRoleController.class)); // Handler in sub-sub-package

        // fail if any class starts with io.javalin
        classes.forEach(c -> assertFalse(c.getPackage().getName().startsWith("io.javalin")));
    }



}