package helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import fixtures.CwmsDataApiSetupCallback;
import java.io.BufferedWriter;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import org.apache.catalina.core.StandardContext;

/** Test-only server mode: external clients drive HTTP while this JVM records its own resource use. */
final class BenchmarkServerMonitor {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private BenchmarkServerMonitor() {
    }

    static void serve(Path output, String baseUrl, List<String> series, Instant start, int points) throws Exception {
        Files.createDirectories(output);
        StandardContext context = (StandardContext) CwmsDataApiSetupCallback.getTestSessionManager()
                .getContext().getParent().findChild(System.getProperty("warContext"));
        Object datasource = context.getNamingContextListener().getEnvContext().lookup("jdbc/CWMS3");
        Object pool = datasource.getClass().getMethod("getPool").invoke(datasource);
        Map<String, Object> ready = new LinkedHashMap<>();
        ready.put("baseUrl", baseUrl);
        ready.put("series", series);
        ready.put("start", start.toString());
        ready.put("points", points);
        ready.put("pid", ProcessHandle.current().pid());
        ready.put("heapMaxBytes", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax());
        ready.put("processors", Runtime.getRuntime().availableProcessors());
        ready.put("javaVersion", System.getProperty("java.version"));
        ready.put("apiClasses", cwms.cda.data.dao.TimeSeriesDaoImpl.class
                .getProtectionDomain().getCodeSource().getLocation().toString());
        ready.put("poolMaxActive", datasource.getClass().getMethod("getMaxActive").invoke(datasource));
        ready.put("poolMaxWaitMs", datasource.getClass().getMethod("getMaxWait").invoke(datasource));
        ready.put("apiTimeoutMs", Integer.getInteger("cwms.cda.api.apiTimeoutMs", 45000));
        System.gc();
        try (BufferedWriter samples = Files.newBufferedWriter(output.resolve("server-metrics.jsonl"))) {
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(output.resolve("ready.json").toFile(), ready);
            System.out.println("External load server ready: " + baseUrl);
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MINUTES.toNanos(
                    Integer.getInteger("benchmark.serverMinutes", 45));
            while (!Files.exists(output.resolve("stop")) && System.nanoTime() < deadline) {
                Map<String, Object> sample = new LinkedHashMap<>();
                sample.put("epochMs", System.currentTimeMillis());
                sample.put("heapUsedBytes", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
                sample.put("threads", ManagementFactory.getThreadMXBean().getThreadCount());
                sample.put("cpuTimeNs", ((com.sun.management.OperatingSystemMXBean)
                        ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime());
                for (String metric : new String[]{"Active", "Idle", "Size", "WaitCount"}) {
                    sample.put("pool" + metric, pool.getClass().getMethod("get" + metric).invoke(pool));
                }
                ForkJoinPool common = ForkJoinPool.commonPool();
                sample.put("commonPoolSize", common.getPoolSize());
                sample.put("commonPoolActive", common.getActiveThreadCount());
                sample.put("commonPoolQueued", common.getQueuedSubmissionCount());
                sample.put("commonPoolTasks", common.getQueuedTaskCount());
                long gcCount = 0;
                long gcMs = 0;
                for (var gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                    gcCount += Math.max(0, gc.getCollectionCount());
                    gcMs += Math.max(0, gc.getCollectionTime());
                }
                sample.put("gcCount", gcCount);
                sample.put("gcMs", gcMs);
                samples.write(MAPPER.writeValueAsString(sample));
                samples.newLine();
                samples.flush();
                Thread.sleep(250);
            }
        }
    }
}
