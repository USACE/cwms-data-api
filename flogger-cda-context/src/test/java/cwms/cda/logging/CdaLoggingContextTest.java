package cwms.cda.logging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.google.common.flogger.FluentLogger;
import com.google.common.flogger.MetadataKey;
import com.google.common.flogger.context.ScopedLoggingContexts;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static cwms.cda.logging.CdaLoggingContext.TRACE_ID;

class CdaLoggingContextTest {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    private static final MemoryAppender memoryAppender = new MemoryAppender();

    private static final MetadataKey<String> NESTED_KEY = MetadataKey.single("nestedKey", String.class);

    @BeforeAll
    static void setup_logger() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        var logbackLogger = context.getLogger(CdaLoggingContextTest.class);
        memoryAppender.setContext(context);
        memoryAppender.setName(CdaLoggingContextTest.class.getName());
        memoryAppender.start();

        logbackLogger.addAppender(memoryAppender);
        logbackLogger.setAdditive(false);
    }

    @AfterEach
    void clear_logger() {
        memoryAppender.clear();
    }

    @Test
    void test_basic_logging_context() {
        ScopedLoggingContexts.newContext()
                             .withMetadata(TRACE_ID, "test-trace-id")
                             .run(() -> logger.atInfo().log("hello"));

        var messages = memoryAppender.getEvents();
        assertFalse(messages.isEmpty());
        boolean foundMsg = false;
        boolean foundId = false;
        for (var msg: messages) {
            if (msg.getMessage().contains("hello")) {
                foundMsg = true;
            }
            if ("test-trace-id".equals(msg.getMDCPropertyMap().get("traceId"))) {
                foundId = true;
            }

        }
        assertTrue(foundMsg, "Message was not in log.");
        assertTrue(foundId, "trace id was not present in the log messages");
    }

    @Test
    void test_nested_scopes() {
        final var ctx = ScopedLoggingContexts.newContext()
                             .withMetadata(TRACE_ID, "test-trace-id");

        ctx.run(() -> {
            logger.atInfo().log("hello not nested");
            ScopedLoggingContexts.newContext()
                                 .withMetadata(NESTED_KEY, "nestEgg")
                                 .run(() -> logger.atInfo().log("hello nested"));
        });

        final var messages = memoryAppender.getEvents();
        assertEquals(2, messages.size());
        final var notNestedEvent = messages.get(0);
        final var nestedEvent = messages.get(1);

        assertFalse(notNestedEvent.getMDCPropertyMap().containsKey(NESTED_KEY.getLabel()));
        assertEquals("test-trace-id", notNestedEvent.getMDCPropertyMap().get(TRACE_ID.getLabel()));

        assertEquals("nestEgg", nestedEvent.getMDCPropertyMap().get(NESTED_KEY.getLabel()));
        assertEquals("test-trace-id", nestedEvent.getMDCPropertyMap().get(TRACE_ID.getLabel()));
    }

    @Test
    void test_nested_scope_in_theory() throws Exception {
        try (var ctx = ScopedLoggingContexts.newContext()
                                            .withMetadata(TRACE_ID, "test-trace-id")
                                            .install()) {
            logger.atInfo().log("before thread");
            final var forThreadCtx = ScopedLoggingContexts.newContext();
            var thread = new Thread(() -> {
                forThreadCtx.withMetadata(NESTED_KEY, "threadEgg")
                            .run(() -> logger.atInfo().log("in thread"));
            });

            thread.start();
            thread.join();
        
            final var messages = memoryAppender.getEvents();
            assertEquals(2, messages.size());
            final var notNestedEvent = messages.get(0);
            final var nestedEvent = messages.get(1);

            assertFalse(notNestedEvent.getMDCPropertyMap().containsKey(NESTED_KEY.getLabel()));
            assertEquals("test-trace-id", notNestedEvent.getMDCPropertyMap().get(TRACE_ID.getLabel()));

            assertEquals("threadEgg", nestedEvent.getMDCPropertyMap().get(NESTED_KEY.getLabel()));
            assertEquals("test-trace-id", nestedEvent.getMDCPropertyMap().get(TRACE_ID.getLabel()));

        }
    }


    public static class MemoryAppender extends AppenderBase<ILoggingEvent> {
        private final List<ILoggingEvent> events = Collections.synchronizedList(new ArrayList<>());


        @Override
        protected void append(ILoggingEvent event) {
            event.prepareForDeferredProcessing();
            events.add(event);
        }

        public List<ILoggingEvent> getEvents() {
            return new ArrayList<>(events); // Return a copy to avoid ConcurrentModificationException
        }

        public void clear() {
            events.clear();
        }
    }
}
