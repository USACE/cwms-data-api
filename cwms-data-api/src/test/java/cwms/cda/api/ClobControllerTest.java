package cwms.cda.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
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
import io.javalin.mock.ContextMock;
import io.javalin.router.Endpoint;
import io.javalin.config.ContextResolverConfig;
import io.javalin.config.Key;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.owasp.html.PolicyFactory;

public class ClobControllerTest extends ControllerTest {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();


    @Test
    void bad_format_returns_501() throws Exception {

        ClobController controller = spy(new ClobController(new MetricRegistry()));
        var url = "http://localhost:7000/cwms-data/clobs";

        final var conn = getTestConnection();
        final AtomicReference<FormattingException> thrownException = new AtomicReference<>(null);
        var executor = ContextMock.create(config -> {
                            config.getReq().contentType = "*";
                            config.getReq().requestURL = url;
                            config.getReq().method = HandlerType.GET.name();
                            config.getReq().addHeader(Header.ACCEPT, "BAD FORMAT");
                            config.getReq().attributes.put("DataSource", conn);
                            config.javalinConfig(c -> {
                                c.routes.exception(FormattingException.class, (e, ctx) -> thrownException.set(e));
                            });
                        })
                                 .build("/cwms-data/clobs");
        var endpoint = Endpoint.create(HandlerType.GET, "/cwms-data/clobs").handler(controller::getAll);
        endpoint.handle(executor);
        assertNotNull(thrownException.get());
        
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
