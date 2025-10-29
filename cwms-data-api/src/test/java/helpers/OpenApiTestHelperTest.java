package helpers;

import cwms.cda.api.OfficeController;
import cwms.cda.api.TextTimeSeriesValueController;
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
        OpenApiDocInfo<OfficeController> info = OpenApiTestHelper.readDocParams(OfficeController.class, m);
        assertNotNull(info);
        assertEquals(2, info.getQueryParameters().size());
        assertTrue(info.getQueryParameters().contains("format"));
        assertTrue(info.getQueryParameters().contains("has-data"));

        assertEquals(0, info.getPathParameters().size());

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
    void test_helper_for_interface() {
        List<OpenApiDocInfo<OfficeController>> crudDocInfo = OpenApiTestHelper.readOpenApiDocs(CrudHandler.class, OfficeController.class);
        assertEquals(5, crudDocInfo.size());

        List<OpenApiDocInfo<TextTimeSeriesValueController>> handlerDocInfo = OpenApiTestHelper.readOpenApiDocs(Handler.class, TextTimeSeriesValueController.class);
        assertEquals(1, handlerDocInfo.size());
    }

    @Test
    void test_find_handlers() {
        List<Class<CrudHandler>> crudHandlers = OpenApiTestHelper.findClassesOfType(CrudHandler.class);
        assertTrue(crudHandlers.contains(OfficeController.class));  // CrudHandler
        assertTrue(crudHandlers.contains(RatingController.class));  // CrudHandler in sub-package
        assertTrue(crudHandlers.contains(UsersController.class));   // CrudHandler in sub-sub-package

        List<Class<Handler>> handlers = OpenApiTestHelper.findClassesOfType(Handler.class);
        assertTrue(handlers.contains(TextTimeSeriesValueController.class));  // Handler
        assertTrue(handlers.contains(WaterUserDeleteController.class));      // Handler in sub-package
        assertTrue(handlers.contains(AddRoleController.class));              // Handler in sub-sub-package

        // Using assertAll allows us to test all cases and provide feedback for all cases instead of failing on the first failure.
        assertAll(crudHandlers.stream().map(handler -> () -> assertFalse(handler.getPackage().getName().startsWith("io.javalin"))));
        assertAll(handlers.stream().map(handler -> () -> assertFalse(handler.getPackage().getName().startsWith("io.javalin"))));
    }
}