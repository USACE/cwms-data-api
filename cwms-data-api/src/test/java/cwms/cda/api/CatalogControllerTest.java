/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import cwms.cda.formatters.FormattingException;
import fixtures.TestHttpServletResponse;
import fixtures.TestServletInputStream;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import io.javalin.http.HttpCode;
import io.javalin.http.util.ContextUtil;
import io.javalin.plugin.json.JavalinJackson;
import io.javalin.plugin.json.JsonMapperKt;
import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class CatalogControllerTest extends ControllerTest{

    @Disabled // get all the infrastructure in place first.
    @ParameterizedTest
    @ValueSource(strings = {"blurge,","appliation/json+fred"})
    public void bad_formats_return_501(String format ) throws Exception {
        final String testBody = "test";
        CatalogController controller = spy(new CatalogController(OpenTelemetry.noop()));

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        HashMap<String,Object> attributes = new HashMap<>();
        attributes.put(ContextUtil.maxRequestSizeKey,Integer.MAX_VALUE);

        when(request.getInputStream()).thenReturn(new TestServletInputStream(testBody));
        when(request.getAttribute("database")).thenReturn(this.conn);
        when(request.getRequestURI()).thenReturn("/catalog/TIMESERIES");

        Context context = ContextUtil.init(request,response,"*",new HashMap<String,String>(), HandlerType.GET,attributes);
        context.attribute("database",this.conn);


        assertNotNull( context.attribute("database"), "could not get the connection back as an attribute");

        when(request.getHeader(Header.ACCEPT)).thenReturn("BAD FORMAT");

        assertThrows( FormattingException.class, () -> {
            controller.getAll(context);
        });
    }

    @Test
    public void catalog_returns_only_original_ids_by_default() throws Exception {
        CatalogController controller = new CatalogController(OpenTelemetry.noop());
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = new TestHttpServletResponse();

        HashMap<String,Object> attributes = new HashMap<>();
        attributes.put(ContextUtil.maxRequestSizeKey,Integer.MAX_VALUE);
        attributes.put(JsonMapperKt.JSON_MAPPER_KEY,new JavalinJackson());
        attributes.put("PolicyFactory", this.sanitizer);

        when(request.getInputStream()).thenReturn(new TestServletInputStream(""));
        when(request.getAttribute("database")).thenReturn(this.conn);
        when(request.getRequestURI()).thenReturn("/catalog/TIMESERIES");
        when(request.getHeader(Header.ACCEPT)).thenReturn("application/json;version=2");

        Context context = ContextUtil.init(request,
                                           response,
                                           "*",
                                           new HashMap<String,String>(),
                                           HandlerType.GET,
                                           attributes);
        context.attribute("database",this.conn);

        controller.getOne(context, CatalogableEndpoint.TIMESERIES.getValue());

        assertEquals(HttpCode.OK.getStatus(), response.getStatus(), "200 OK was not returned");
        assertNotNull(response.getOutputStream(), "Output stream wasn't created");

    }
}
