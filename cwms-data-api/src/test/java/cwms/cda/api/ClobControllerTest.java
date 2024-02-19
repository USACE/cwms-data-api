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
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import io.javalin.http.util.ContextUtil;
import io.javalin.plugin.json.JavalinJackson;
import io.javalin.plugin.json.JsonMapperKt;
import java.util.HashMap;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;

public class ClobControllerTest extends ControllerTest {
    public static final FluentLogger logger = FluentLogger.forEnclosingClass();


    @Test
    void bad_format_returns_501() throws Exception {

        final String testBody = "";
        ClobController controller = spy(new ClobController(new MetricRegistry()));
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        HashMap<String, Object> attributes = new HashMap<>();
        attributes.put(ContextUtil.maxRequestSizeKey, Integer.MAX_VALUE);
        attributes.put(JsonMapperKt.JSON_MAPPER_KEY, new JavalinJackson());

        when(request.getInputStream()).thenReturn(new TestServletInputStream(testBody));

        Context context = ContextUtil.init(request, response, "*", new HashMap<>(),
                HandlerType.GET, attributes);
        context.attribute("database", getTestConnection());

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

        ClobController controller = new ClobController(new MetricRegistry());

        Clob clob = controller.deserialize(input, Formats.JSONV2);
        assertNotNull(clob);
        assertEquals("MYOFFICE", clob.getOfficeId());
        assertEquals("MYID", clob.getId());
        assertEquals("MYDESC", clob.getDescription());
        assertEquals("MYVALUE", clob.getValue());
    }


}
