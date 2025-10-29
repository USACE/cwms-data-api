package helpers;

import cwms.cda.api.OfficeController;
import cwms.cda.api.TextTimeSeriesValueController;
import cwms.cda.api.TimeSeriesController;
import cwms.cda.api.auth.users.UsersController;
import cwms.cda.api.auth.users.roles.AddRoleController;
import cwms.cda.api.rating.RatingController;
import cwms.cda.api.watersupply.WaterUserDeleteController;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Handler;
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
        List<Class<CrudHandler>> crudHandlers = OpenApiTestHelper.findClassesOfType(CrudHandler.class, "cwms.cda.api");
        assertNotNull(crudHandlers);
        assertTrue(crudHandlers.contains(OfficeController.class));  // CrudHandler
        assertTrue(crudHandlers.contains(RatingController.class));  // CrudHandler in sub package
        assertTrue(crudHandlers.contains(UsersController.class));   // CrudHandler in sub-sub package

        List<Class<Handler>> handlers = OpenApiTestHelper.findClassesOfType(Handler.class, "cwms.cda.api");
        assertTrue(handlers.contains(TextTimeSeriesValueController.class));  // Handler
        assertTrue(handlers.contains(WaterUserDeleteController.class));      // Handler in sub-package
        assertTrue(handlers.contains(AddRoleController.class));              // Handler in sub-sub-package

        // fail if any class starts with io.javalin
        crudHandlers.forEach(c -> assertFalse(c.getPackage().getName().startsWith("io.javalin")));
    }



}