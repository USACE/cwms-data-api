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

import com.google.common.flogger.FluentLogger;
import com.rometools.rome.feed.synd.SyndContent;
import com.rometools.rome.feed.synd.SyndContentImpl;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndEntryImpl;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.feed.synd.SyndFeedImpl;
import com.rometools.rome.io.SyndFeedOutput;
import cwms.cda.formatters.FormattingException;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import oracle.jms.AQjmsFactory;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.component.jms.JmsMessage;
import org.apache.camel.impl.DefaultCamelContext;
import org.jetbrains.annotations.NotNull;

import javax.jms.JMSException;
import javax.jms.TopicConnectionFactory;
import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class MessageQueueHandler implements Handler {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private final Queue<SyndEntry> feedEntries = new LinkedList<>();
    private final DataSource cwms;
    private DefaultCamelContext camelContext;

    public MessageQueueHandler(DataSource dataSource) {
        cwms = dataSource;
        try {
            setupCamel();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to setup Queues", e);
        }

    }

    private void setupCamel() throws Exception {
        camelContext = new DefaultCamelContext();
        TopicConnectionFactory connectionFactory = AQjmsFactory.getTopicConnectionFactory(new DataSourceWrapper(cwms), true);
        camelContext.addComponent("oracleAQ", JmsComponent.jmsComponent(connectionFactory));
        camelContext.addRoutes(new RouteBuilder() {
            public void configure() {
                from("oracleAQ:topic:CWMS_20.SWT_TS_STORED?durableSubscriptionName=CDA_SWT_TS_STORED&clientId=CDA")
                        .log("Received message from ActiveMQ.Queue : ${body}")
                        .process(new MapMessageToJsonProcessor())
                        .process(MessageQueueHandler.this::sendToSyndication);
            }
        });
        camelContext.start();
    }

    //Called from apache camel
    public void sendToSyndication(Exchange exchange) {
        JmsMessage message = (JmsMessage) exchange.getIn();
        String exchangeId = exchange.getExchangeId();
        // Create an entry
        SyndEntry entry = new SyndEntryImpl();
        entry.setTitle(exchangeId);
        try {
            entry.setPublishedDate(new Date(message.getJmsMessage().getJMSTimestamp()));
        } catch (JMSException e) {
            LOGGER.atWarning().withCause(e).log("Error determining JMS message timestamp. Setting to current date");
            entry.setPublishedDate(new Date());
        }
        entry.setAuthor("CWMS Data API");
        SyndContent content = new SyndContentImpl();
        content.setType("application/json");
        content.setValue(message.getBody(String.class));
        entry.setDescription(content);
        synchronized (feedEntries) {
            //Change to an aged queue
            feedEntries.add(entry);
            if (feedEntries.size() > 10_000) {
                feedEntries.remove();
            }
        }
    }

    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        SyndFeed feed = new SyndFeedImpl();
        String formatHeader = ctx.header(Header.ACCEPT);
        if ("application/rss+xml".equalsIgnoreCase(formatHeader)) {
            feed.setFeedType("rss_2.0");
        } else if ("application/atom+xml".equalsIgnoreCase(formatHeader)) {
            feed.setFeedType("atom_1.0");
        } else {
            throw new FormattingException("Accept header: " + formatHeader + " not accepted. Use application/rss+xml or application/atom+xml");
        }
        //TODO: use path parameter to determine which queue to receive message from
        //TODO: use query parameter to setup message filters
        feed.setTitle("Test RSS/Atom Feed");
        feed.setDescription("A test feed SWT_TS_STORED Oracle Queue messages.");
        feed.setLink("http://example.com");
        synchronized (feedEntries) {
            feed.setEntries(new ArrayList<>(feedEntries));
        }
        SyndFeedOutput output = new SyndFeedOutput();
        ctx.contentType("application/rss+json");
        ctx.result(output.outputString(feed));
    }
}
