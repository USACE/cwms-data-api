package cwms.radar.security;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;

import org.junit.jupiter.api.Test;

public class CwmsNormalAuthorizorTest {

    @Test
    public void test_not_in_role_throws_exception(){
        HttpServletRequest request = mock(HttpServletRequest.class);

        when(request.isUserInRole("CWMS Users")).thenReturn(false);

        CwmsAuthorizer authorizer = new CwmsNormalAuthorizer();


        assertThrows(CwmsAuthException.class, () -> {
            authorizer.can_perform(request,null);
        });
    }

    @Test
    public void test_in_role_does_not_throw() {
        HttpServletRequest request = mock(HttpServletRequest.class);

        when(request.isUserInRole("CWMS Users")).thenReturn(true);

        CwmsAuthorizer authorizer = new CwmsNormalAuthorizer();


        assertDoesNotThrow( () -> {
            authorizer.can_perform(request,null);
        });
    }
}
