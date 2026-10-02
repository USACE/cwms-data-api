package cwms.cda.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dto.Clob;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import fixtures.TestServletInputStream;
import io.javalin.http.Header;
import io.javalin.http.servlet.MaxRequestSize;
import io.javalin.config.ContextResolverConfig;
import io.javalin.config.Key;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import java.util.HashMap;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.owasp.html.PolicyFactory;

public class ClobControllerTest extends ControllerTest {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();


    @Test
    void bad_format_returns_501() throws Exception {

        final String testBody = "";
        ClobController controller = spy(new ClobController(new MetricRegistry()));
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);

        Map<Key<?>, Object> attributes = new HashMap<>();
        attributes.put(MaxRequestSize.INSTANCE.getMaxRequestSizeKey(), Long.MAX_VALUE);
        attributes.put(new Key<PolicyFactory>("PolicyFactory"), this.sanitizer);
        attributes.put(ContextResolverConfig.Companion.getContextResolverKey$javalin(), new ContextResolverConfig());

        when(request.getInputStream()).thenReturn(new TestServletInputStream(testBody));
        // JooqDao.getDslContext snapshots client-info from the request for connection preparers
        when(request.getMethod()).thenReturn("GET");
        when(request.getRequestURI()).thenReturn("/cwms-data/clobs");
        when(request.getRequestURL()).thenReturn(new StringBuffer("http://localhost:7000/cwms-data/clobs"));
        when(request.getContextPath()).thenReturn("/cwms-data");

        var context = mock(Context.class);
        when(context.req()).thenReturn(request);
        when(context.res()).thenReturn(response);
        when(context.attributeMap()).thenReturn(new HashMap<>());
        when(context.contentType()).thenReturn("*");
        when(context.method()).thenReturn(HandlerType.GET);
        when(context.attribute("database")).thenReturn(getTestConnection());
        


        when(request.getAttribute("database")).thenReturn(getTestConnection());

        assertNotNull(context.attribute("database"), "could not get the connection back as an "
                + "attribute");

        when(request.getHeader(Header.ACCEPT)).thenReturn("BAD FORMAT");

        assertThrows(FormattingException.class, () -> controller.getAll(context));

    }


    @Test
    void testDeserialize() throws JsonProcessingException {
        String input = "{\"office-id\":\"MYOFFICE\",\"id\":\"MYID\",\"description\":\"MYDESC\","
                + "\"value\":\"MYVALUE\"}";

        Clob clob = Formats.parseContent(Formats.parseHeader(Formats.JSONV2, Clob.class),input, Clob.class);
        assertNotNull(clob);
        assertEquals("MYOFFICE", clob.getOfficeId());
        assertEquals("MYID", clob.getId());
        assertEquals("MYDESC", clob.getDescription());
        assertEquals("MYVALUE", clob.getValue());
    }


}
