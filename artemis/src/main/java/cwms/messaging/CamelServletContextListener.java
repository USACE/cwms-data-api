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

import oracle.jms.AQjmsFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;

import javax.annotation.Resource;
import javax.jms.ConnectionFactory;
import javax.jms.TopicConnectionFactory;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.sql.DataSource;

public final class CamelServletContextListener implements ServletContextListener {

    @Resource(name = "jdbc/CWMS3")
    DataSource cwms;
    private DefaultCamelContext camelContext;

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {

        try {
            camelContext = new DefaultCamelContext();
            TopicConnectionFactory connectionFactory = AQjmsFactory.getTopicConnectionFactory(new DataSourceWrapper(cwms), true);
            camelContext.addComponent("oracleAQ", JmsComponent.jmsComponent(connectionFactory));
            ConnectionFactory artemisConnectionFactory = new ActiveMQJMSConnectionFactory("vm://0");
            camelContext.addComponent("artemis", JmsComponent.jmsComponent(artemisConnectionFactory));
            camelContext.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("oracleAQ:topic:CWMS_20.SWT_TS_STORED?durableSubscriptionName=CDA_SWT_TS_STORED&clientId=CDA")
                            .log("Received message from CWMS_20.SWT_TS_STORED : ${body}")
                            //Converting MapMessage to JSON for client processing
                            //We could have an additional routes for different message formats
                            .process(new MapMessageToJsonProcessor(camelContext))
                            .log("Processed message body for Artemis: ${body}")
                            //Artemis REST API requires the JMS type to be Object
                            .to("artemis:topic:CDA_SWT_TS_STORED?jmsMessageType=Object");
                }
            });
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
