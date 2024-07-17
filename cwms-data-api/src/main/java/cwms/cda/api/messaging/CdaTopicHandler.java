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

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.queryParamAsClass;
import static java.util.stream.Collectors.toList;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.Controllers;
import cwms.cda.data.dto.messaging.CdaTopics;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.jetbrains.annotations.NotNull;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public final class CdaTopicHandler implements Handler {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final String TAG = "Messaging";
    private final MetricRegistry metrics;
    private final Histogram requestResultSize;
    private ActiveMQServer artemis;
    private CamelRouter router;

    public CdaTopicHandler(DataSource cwms, MetricRegistry metrics) {
        this.metrics = metrics;
        this.requestResultSize = this.metrics.histogram((name(CdaTopicHandler.class.getName(), RESULTS, SIZE)));
        try {
            File brokerXmlFile = new File("src/test/resources/tomcat/conf/broker.xml").getAbsoluteFile();
            if (brokerXmlFile.exists()) {
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                dbFactory.setExpandEntityReferences(false);
                dbFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
                dbFactory.setNamespaceAware(true);
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                Document doc = dBuilder.parse(brokerXmlFile);
                doc.getDocumentElement().normalize();
                Element rootElement = doc.getDocumentElement();
                FileConfiguration configuration = new FileConfiguration();
                configuration.parse(rootElement, brokerXmlFile.toURI().toURL());
                artemis = ActiveMQServers.newActiveMQServer(configuration);
                router = new CamelRouter(cwms);
                artemis.registerBrokerPlugin(router);
                artemis.setSecurityManager(new ArtemisSecurityManager(cwms));
                artemis.start();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Unable to setup Queues", e);
        }
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
        description = "Request the list of supported CDA topics in alphabetical order. " +
            "Additional information for the host address of the messaging server is also provided.",
        queryParams = {
            @OpenApiParam(name = OFFICE,
                description = "Specifies the owning office. If this field is not "
                    + "specified, matching information from all offices shall be "
                    + "returned."),
        },
        responses = {@OpenApiResponse(status = STATUS_200,
            description = "A list of supported CDA topics.",
            content = {
                @OpenApiContent(type = Formats.JSONV1, from = CdaTopics.class),
                @OpenApiContent(type = Formats.JSON, from = CdaTopics.class)
            })
        },
        method = HttpMethod.GET,
        tags = {TAG}
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            String office = ctx.queryParam(OFFICE);
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, CdaTopics.class);
            Collection<String> topics = router.getTopics(office);
            List<Map<String, Object>> configurations = new ArrayList<>();
            if(artemis.isStarted()) {
                configurations = artemis.getConfiguration().getAcceptorConfigurations().stream()
                    .map(TransportConfiguration::getParams)
                    //Need to filter out the In-VM acceptor
                    .filter(s -> s.containsKey("host"))
                    .collect(toList());
            }
            Set<String> protocols = artemis.getRemotingService().getProtocolFactoryMap().keySet();
            CdaTopics cdaTopics = new CdaTopics(configurations, protocols, topics);
            String result = Formats.format(contentType, cdaTopics);
            ctx.result(result);
            ctx.contentType(contentType.toString());
            requestResultSize.update(result.length());
        }
    }

    public void shutdown() {
        if (artemis != null) {
            try {
                artemis.stop();
            } catch (Exception e) {
                LOGGER.atWarning().withCause(e).log("Unable to stop Artemis server during servlet shutdown");
            }
        }
        if (router != null) {
            try {
                router.stop();
            } catch (Exception e) {
                LOGGER.atWarning().withCause(e).log("Unable to stop Camel Route Handler during servlet shutdown");
            }
        }
    }
}
