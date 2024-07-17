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

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import com.google.common.flogger.FluentLogger;
import cwms.cda.ApiServlet;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dto.auth.ApiKey;
import cwms.cda.security.DataApiPrincipal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.jms.ConnectionFactory;
import javax.jms.TopicConnectionFactory;
import javax.sql.DataSource;
import oracle.jms.AQjmsFactory;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

final class CamelRouter implements ActiveMQServerPlugin {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final String ORACLE_QUEUE_SOURCE = "oracleAQ";
    private static final String ARTEMIS_QUEUE_SOURCE = "artemis";
    private final CamelContext camelContext;
    private final Map<OracleQueue, RouteDefinition> routeDefinitions;
    private final String oracleAqClientId;

    CamelRouter(DataSource cwms) throws Exception {
        oracleAqClientId = getClientId();
        camelContext = initCamel(cwms);
        routeDefinitions = buildRouteDefinitions(cwms);
        camelContext.addRouteDefinitions(routeDefinitions.values());
    }

    private CamelContext initCamel(DataSource cwms) {
        try {
            //wrapped DelegatingDataSource is used because internally AQJMS casts the returned connection
            //as an OracleConnection, but the JNDI pool is returning us a proxy, so unwrap it
            DefaultCamelContext camel = new DefaultCamelContext();
            DataSourceWrapper dataSource = new DataSourceWrapper(cwms);
            TopicConnectionFactory connectionFactory = AQjmsFactory.getTopicConnectionFactory(dataSource, true);
            camel.addComponent(ORACLE_QUEUE_SOURCE, JmsComponent.jmsComponent(connectionFactory));

            DSLContext context = DSL.using(cwms, SQLDialect.ORACLE18C);
            String cdaUser = context
                .connectionResult(c -> c.getMetaData().getUserName());
            String apiKey = createApiKey(context, cdaUser);
            ConnectionFactory artemisConnectionFactory = new ActiveMQJMSConnectionFactory("vm://0", cdaUser, apiKey);
            camel.addComponent(ARTEMIS_QUEUE_SOURCE, JmsComponent.jmsComponent(artemisConnectionFactory));
            camel.start();
            return camel;
        } catch (Exception e) {
            throw new IllegalStateException("Unable to setup Queues", e);
        }
    }

    private Map<OracleQueue, RouteDefinition> buildRouteDefinitions(DataSource cwms) {
        DSLContext create = DSL.using(cwms, SQLDialect.ORACLE18C);
        Field<String> field = field(name("OWNER")).concat(".").concat(field(name("NAME"))).as("queue");
        return create.select(field)
            .from(table(name("DBA_QUEUES")))
            .where(field(name("OWNER")).eq("CWMS_20"))
            .and(field(name("QUEUE_TYPE")).eq("NORMAL_QUEUE"))
            .fetch()
            .stream()
            .map(Record1::component1)
            .distinct()
            .map(OracleQueue::new)
            .collect(toMap(q -> q, this::queueToRoute));
    }

    private RouteDefinition queueToRoute(OracleQueue queue) {
        RouteDefinition routeDefinition = new RouteDefinition();
        String durableSub = (ApiServlet.APPLICATION_TITLE + "_" + queue.getOracleQueueName())
            .replace(" ", "_")
            .replace(".", "_");
        String fromOracleRoute = format("%s:topic:%s?durableSubscriptionName=%s&clientId=%s", ORACLE_QUEUE_SOURCE,
            queue.getOracleQueueName(), durableSub, oracleAqClientId);
        String[] topics = queue.getTopicIds()
            .stream()
            .map(CamelRouter::createArtemisLabel)
            .toArray(String[]::new);
        routeDefinition.id(queue.getOracleQueueName());
        routeDefinition.from(fromOracleRoute)
            .log("Received message from ActiveMQ.Queue : ${body}")
            .process(new MapMessageToJsonProcessor(camelContext))
            .to(topics)
            .autoStartup(false);
        return routeDefinition;
    }

    private static String getClientId() {
        try {
            String host = InetAddress.getLocalHost().getCanonicalHostName().replace("/", "_");
            return "CDA_" + host.replace(".", "_").replace(":", "_");
        } catch (UnknownHostException e) {
            throw new IllegalStateException("Cannot obtain local host name for durable subscription queue setup", e);
        }
    }

    @Override
    public void afterCreateConsumer(ServerConsumer consumer) throws ActiveMQException {
        String routeId = consumer.getQueueAddress().toString();
        String label = createArtemisLabel(routeId);
        List<RouteDefinition> routeDefinition = routeDefinitions.values()
            .stream()
            .filter(r -> r.getOutputs().stream().anyMatch(o -> o.getLabel().equals(label)))
            .collect(toList());
        if (routeDefinition.isEmpty()) {
            throw new ActiveMQException(ActiveMQExceptionType.QUEUE_DOES_NOT_EXIST,
                "Route for id: " + routeId + " does not exit");
        }
        try {
            for (RouteDefinition route : routeDefinition) {
                //Camel handles synchronization internally
                //Calling startRoute on an already started route is innocuous
                camelContext.startRoute(route.getId());
            }
        } catch (Exception e) {
            throw new ActiveMQException("Could not start route: " + routeId, e,
                ActiveMQExceptionType.GENERIC_EXCEPTION);
        }
    }

    Collection<String> getTopics(String office) {
        return routeDefinitions.keySet().stream()
            .filter(q -> office == null || q.office.equalsIgnoreCase(office))
            .map(OracleQueue::getTopicIds)
            .flatMap(Collection::stream)
            .collect(toSet());
    }

    private static String createArtemisLabel(String routeId) {
        return format("%s:topic:%s", ARTEMIS_QUEUE_SOURCE, routeId);
    }

    void stop() throws Exception {
        camelContext.stop();
    }

    private String createApiKey(DSLContext context, String user) {
        AuthDao instance = AuthDao.getInstance(context);
        UUID uuid = UUID.randomUUID();
        DataApiPrincipal principal = new DataApiPrincipal(user, new HashSet<>());
        ZonedDateTime now = ZonedDateTime.now();
        //TODO: Expiration should be handled more gracefully.
        // This assumes no new queues are accessed after three months of uptime
        //TODO: cda_camel_invm needs to be unique per instance of CDA. Not sure how to handle that at the moment.
        // for now using current epoch millis. This unfortunately leaves old keys between restarts.
        String keyName = "cda_camel_invm_" + Instant.now().toEpochMilli();
        ApiKey apiKey = new ApiKey(user, keyName, uuid.toString(), now, now.plusMonths(3));
        return instance.createApiKey(principal, apiKey).getApiKey();
    }

    private static final class OracleQueue {
        private static final Pattern ORACLE_QUEUE_PATTERN =
            Pattern.compile("CWMS_20\\.(?<office>[A-Z]+)_(?<queueGroup>.*)");
        private final String oracleQueueName;
        private final String office;
        private final String queueGroup;

        private OracleQueue(String oracleQueueName) {
            this.oracleQueueName = oracleQueueName;
            Matcher matcher = ORACLE_QUEUE_PATTERN.matcher(oracleQueueName);
            if (matcher.matches()) {
                this.office = matcher.group("office");
                this.queueGroup = matcher.group("queueGroup");
            } else {
                LOGGER.atInfo().log("Oracle queue:" + oracleQueueName + " did not match standard pattern: " +
                    ORACLE_QUEUE_PATTERN.pattern() + " Artemis topic will use the Oracle queue name as-is.");
                this.office = null;
                this.queueGroup = null;
            }
        }

        private String getOracleQueueName() {
            return this.oracleQueueName;
        }

        private Set<String> getTopicIds() {
            Set<String> retval = new HashSet<>();
            if (this.office != null && queueGroup != null) {
                retval.add("CDA." + this.office + ".ALL");
                retval.add("CDA." + this.office + "." + this.queueGroup);
            } else {
                retval.add(this.oracleQueueName);
            }
            return retval;
        }
    }
}
