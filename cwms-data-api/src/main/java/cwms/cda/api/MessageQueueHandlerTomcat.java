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

package cwms.cda.api;

import com.google.common.flogger.FluentLogger;
import cwms.cda.datasource.DelegatingDataSource;
import oracle.jdbc.driver.OracleConnection;
import oracle.jms.AQjmsFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;

import javax.annotation.Resource;
import javax.jms.TopicConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.servlet.AsyncContext;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@WebServlet(urlPatterns = {"/topics/*"}, asyncSupported = true)
public class MessageQueueHandlerTomcat extends HttpServlet {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private final Map<String, Set<HttpServletResponse>> clients = new ConcurrentHashMap<>();
    private final CamelContext camel;
    @Resource(name = "jdbc/CWMS3")
    DataSource cwms;

    public MessageQueueHandlerTomcat() {
        try {

            Context initContext = new InitialContext();
            Context envContext = (Context) initContext.lookup("java:/comp/env");
            cwms = (DataSource) envContext.lookup("jdbc/CWMS3");
            this.camel = setupQueuing();
        } catch (
                Exception e) {
            throw new IllegalStateException("Unable to setup Queues", e);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        AsyncContext asyncCtx = req.startAsync();
        asyncCtx.setTimeout(0); // disables timeout
        String pathInfo = req.getPathInfo(); // will return "/{queue}"
        String queue = pathInfo.substring(1); // removes the leading "/"
        if (isValidQueue(queue)) {
            clients.computeIfAbsent(queue, q -> Collections.synchronizedSet(new HashSet<>()))
                    .add(resp);
            addCamelRoute(queue);
        } else {
            throw new IOException("Queue does not exist: " + queue);
        }
    }

    private synchronized void addCamelRoute(String queue) {
        Route route = camel.getRoute(queue);
        if (route == null) {
            try {
                camel.addRoutes(new RouteBuilder() {
                    public void configure() {
                        from("oracleAQ:topic:" + queue + "?durableSubscriptionName=CDA_HELLO_WORLD&clientId=CDA")
                                .id(queue)
                                .log("Received message from " + queue + " : ${body}")
                                .bean(MessageQueueHandlerTomcat.this, "sendToQueue('" + queue + "', ${body})", true);
                    }
                });
                camel.startAllRoutes();
            } catch (Exception e) {
                throw new IllegalStateException("Internal error subscribing to queue: " + queue, e);
            }
        }
    }

    private synchronized void clientClosed(HttpServletResponse client, String queue) {
        Set<HttpServletResponse> seeClient = clients.get(queue);
        seeClient.remove(client);
        if (seeClient.isEmpty()) {
            try {
                camel.removeRoute(queue);
            } catch (Exception e) {
                LOGGER.atWarning().withCause(e)
                        .log("Internal error removing camel route with it: " + queue + " after last subscriber was closed");
            }
        }
    }

    private boolean isValidQueue(String queue) {
        //TODO validate queue
        return true;
    }

    //Called from apache camel
    public void sendToQueue(String queue, String message) {
        Set<HttpServletResponse> sseClients = clients.get(queue);
        if (sseClients != null) {
            sseClients.forEach(c -> {
                try {
                    c.setContentType("text/event-stream");
                    c.setCharacterEncoding("UTF-8");
                    PrintWriter writer = c.getWriter();
                    //SSE needs the data: prefix and double newline suffix
                    writer.write("data: " + message + "\n\n");
                    writer.flush();
                } catch (IOException ex) {
                    LOGGER.atInfo().withCause(ex).log("Could not send message to client. Closing client connection");
                    clientClosed(c, queue);
                }
            });
        }
    }

    private CamelContext setupQueuing() throws Exception {
        //wrapped DelegatingDataSource is used because internally AQJMS casts the returned connection
        //as an OracleConnection, but the JNDI pool is returning us a proxy, so unwrap it
        CamelContext camelContext = new DefaultCamelContext();
        TopicConnectionFactory connectionFactory = AQjmsFactory.getTopicConnectionFactory(new DelegatingDataSource(cwms) {
            @Override
            public Connection getConnection() throws SQLException {
                return super.getConnection().unwrap(OracleConnection.class);
            }

            @Override
            public Connection getConnection(String username, String password) throws SQLException {
                return super.getConnection(username, password).unwrap(OracleConnection.class);
            }
        }, true);
        camelContext.addComponent("oracleAQ", JmsComponent.jmsComponent(connectionFactory));
        camelContext.start();
        return camelContext;
    }
}
