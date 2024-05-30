/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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

package cwms.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.component.jms.JmsMessage;
import org.apache.camel.impl.DefaultMessage;

import javax.jms.MapMessage;
import javax.jms.TextMessage;
import java.util.Map;

final class MapMessageToJsonProcessor implements Processor {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final CamelContext context;

    MapMessageToJsonProcessor(CamelContext context) {
        this.context = context;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(Exchange exchange) throws Exception {
        Message inMessage = exchange.getIn();
        //If we use types other than MapMessage or TextMessage, we'd need to handle here
        if (((JmsMessage) inMessage).getJmsMessage() instanceof MapMessage) {
            Map<String, Object> map = inMessage.getBody(Map.class);
            String payload = null;

            if (map != null) {
                payload = OBJECT_MAPPER.writeValueAsString(map);
            }
            inMessage.setBody(payload);
            inMessage.setHeader(Exchange.CONTENT_TYPE, "application/json");
        }
    }
}
