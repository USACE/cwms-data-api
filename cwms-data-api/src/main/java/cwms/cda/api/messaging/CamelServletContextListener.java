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

package cwms.cda.api.messaging;

import cwms.cda.datasource.DelegatingDataSource;
import oracle.jdbc.driver.OracleConnection;
import oracle.jms.AQjmsFactory;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;

import javax.annotation.Resource;
import javax.jms.ConnectionFactory;
import javax.jms.TopicConnectionFactory;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletException;
import javax.sql.DataSource;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.SQLException;

public final class CamelServletContextListener implements ServletContextListener {

    @Resource(name = "jdbc/CWMS3")
    DataSource cwms;
    private DefaultCamelContext camelContext;

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        try {
            //wrapped DelegatingDataSource is used because internally AQJMS casts the returned connection
            //as an OracleConnection, but the JNDI pool is returning us a proxy, so unwrap it
            CamelContext camelContext = new DefaultCamelContext();
            TopicConnectionFactory connectionFactory = AQjmsFactory.getTopicConnectionFactory(new DelegatingDataSource(cwms)
            {
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
            //TODO: determine how the port is configured
            String activeMqUrl = "tcp://" + InetAddress.getLocalHost().getHostName() + ":61616";
            ActiveMQServer server = ActiveMQServers.newActiveMQServer(new ConfigurationImpl()
                    .addAcceptorConfiguration("tcp", activeMqUrl)
                    .setPersistenceEnabled(true)
                    .setJournalDirectory("build/data/journal")
                    //Need to update to verify roles
                    .setSecurityEnabled(false)
                    .addAcceptorConfiguration("invm", "vm://0"));
            ConnectionFactory artemisConnectionFactory = new ActiveMQJMSConnectionFactory("vm://0");
            camelContext.addComponent("artemis", JmsComponent.jmsComponent(artemisConnectionFactory));
            camelContext.addRoutes(new RouteBuilder() {
                public void configure() {
                    //TODO: configure Oracle Queue name for office
                    //TODO: determine durable subscription name - should be unique to CDA instance?
                    //TODO: determine clientId - should be unique to CDA version?
                    from("oracleAQ:topic:CWMS_20.SWT_TS_STORED?durableSubscriptionName=CDA_SWT_TS_STORED&clientId=CDA")
                            .log("Received message from ActiveMQ.Queue : ${body}")
                            //TODO: define standard naming
                            //TODO: register artemis queue names with Swagger UI
                            .to("artemis:topic:ActiveMQ.Queue");
                }
            });
            server.start();
            camelContext.start();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to setup Queues", e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        try {
            camelContext.stop();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to stop Camel context during servlet shutdown");
        }
    }
}
