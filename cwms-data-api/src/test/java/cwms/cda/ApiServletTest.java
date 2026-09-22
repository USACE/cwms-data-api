package cwms.cda;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import cwms.cda.api.auth.userlists.UserListController;
import io.javalin.http.Handler;
import io.javalin.http.HandlerEntry;
import io.javalin.http.HandlerType;
import io.javalin.http.PathMatcher;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import java.util.List;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class ApiServletTest {

    @Test
    void test_user_list_operations_are_grouped_under_user_management() {
        PathItem path = new PathItem()
                .get(new Operation())
                .post(new Operation())
                .delete(new Operation());

        ApiServlet.setUserListTags("/user/list/{user-list-id}/members", path);

        path.readOperations().forEach(operation ->
                assertEquals(List.of(UserListController.TAG), operation.getTags()));
    }

    @Test
    void test_other_operations_are_not_retagged_as_user_management() {
        Operation operation = new Operation().addTagsItem("Other");
        PathItem path = new PathItem().get(operation);

        ApiServlet.setUserListTags("/users/{user-name}", path);

        assertEquals(List.of("Other"), operation.getTags());
    }

    @Test
    public void test_office_from_context_hq(){
        String office;

        office = ApiServlet.officeFromContext("/cwms-data");
        assertEquals("HQ", office, "failed to get HQ result cwms-data context");

        office = ApiServlet.officeFromContext("");
        assertEquals("HQ", office, "failed to get HQ result on root context");
    }

    @ParameterizedTest
    @CsvSource( value = { "/spk-data,SPK", "/nwdm-data,NWDM", "/nww-data,NWW", "/swt-data,SWT"} )
    void test_office_from_context_district(String context, String office) {
        String returnedOffice = ApiServlet.officeFromContext(context);
        assertEquals(office, returnedOffice, "failed to process an office context correctly");
    }

    @Test
    public void test_adding_two_matchers_matches_both() {

        PathMatcher matcher = new PathMatcher();
        Handler handler = (ctx) -> {};
        matcher.add(new HandlerEntry(HandlerType.AFTER, "/offices/{office}", true, handler, handler));
        matcher.add(new HandlerEntry(HandlerType.AFTER, "/offices", true, handler, handler));

        String[] testPaths = new String[]{ "/offices", "/offices/", "/offices/SPK", "/offices/SPK/"};

        for (String testPath : testPaths) {
            List<HandlerEntry> matches = matcher.findEntries(HandlerType.AFTER, testPath);
            assertNotNull(matches, "did not find match " + testPath);
            assertFalse(matches.isEmpty(), testPath+" should have matched");
        }


    }

    @Test
    @Disabled ("can't figure out a way to have one pattern match getOne and getAll style paths")
    public void test_with_pattern() {
// This is how ApiServlet might make afterPath
//        String crudPath = "/offices/{office}";
//        // Lets see if we can use a regex to make something that matches
//        String regex = "(.*)(\\{.*})?"; //"(.*)(\\{.*\\})";
//        String afterPath = crudPath.replaceAll(regex,"$1*");

        // skip trying to build the pattern from the input in the test
        // and just see what inputs to javalin will match
        String afterPath;
//        afterPath= "/offices/{office}"; // does not match /offices or /offices/
//        afterPath= "/offices/*";  // does not match /offices
//        afterPath= "/offices/{office}*";  // does not match /offices or /offices/
//        afterPath= "/offices/{office}**"; // this pattern triggers an exception
        afterPath= "/offices/*"; // does not match /offices


        PathMatcher matcher = new PathMatcher();
        Handler handler = (ctx) -> {};
        matcher.add(new HandlerEntry(HandlerType.AFTER, afterPath, true, handler, handler));

        String[] testPaths = new String[]{
                "/offices",
                "/offices/", "/offices/SPK", "/offices/SPK/"};

        for (String testPath : testPaths) {
            List<HandlerEntry> matches = matcher.findEntries(HandlerType.AFTER, testPath);
            assertNotNull(matches, "did not find match " + testPath);
            assertFalse(matches.isEmpty(), testPath+" should have matched");
        }

    }


    @Test
    public void test_match_with_internal_resource() {
        PathMatcher matcher = new PathMatcher();
        Handler handler = (ctx) -> {};
        matcher.add(new HandlerEntry(HandlerType.AFTER, "/levels/{level-id}/timeseries", true, handler, handler));

        List<HandlerEntry> matches  = matcher.findEntries(HandlerType.AFTER, "/levels/doesthismatch/timeseries");
        assertNotNull(matches);
        assertFalse(matches.isEmpty());

    }

    @Test
    public void test_match_with_internal_star() {
        PathMatcher matcher = new PathMatcher();
        Handler handler = (ctx) -> {};
        matcher.add(new HandlerEntry(HandlerType.AFTER, "/levels/*/timeseries", true, handler, handler));

        List<HandlerEntry> matches  = matcher.findEntries(HandlerType.AFTER, "/levels/doesthismatch/timeseries");
        assertNotNull(matches);
        assertFalse(matches.isEmpty());

    }


}

