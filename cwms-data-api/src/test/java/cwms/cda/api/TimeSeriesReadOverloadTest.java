package cwms.cda.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import cwms.cda.ApiServlet;
import cwms.cda.data.dao.TimeSeriesDao;
import io.javalin.http.Context;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

class TimeSeriesReadOverloadTest {
    @Test
    void rejectsBeforeDatabaseAccessAndReleasesPermitOnFailure() throws Exception {
        CountDownLatch admitted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        MetricRegistry metrics = new MetricRegistry();
        TimeSeriesController controller = new TimeSeriesController(metrics, 1) {
            @Override
            protected TimeSeriesDao getTimeSeriesDao(DSLContext dsl) {
                admitted.countDown();
                try {
                    assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(ex);
                }
                throw new IllegalArgumentException("test request failure");
            }
        };
        DataSource source = mock(DataSource.class);
        Context accepted = mock(Context.class, Answers.RETURNS_SELF);
        when(accepted.attribute(ApiServlet.DATA_SOURCE)).thenReturn(source);
        when(accepted.url()).thenReturn("http://localhost/timeseries");
        when(accepted.path()).thenReturn("/timeseries");
        Context rejected = mock(Context.class, Answers.RETURNS_SELF);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            final Future<?> read = worker.submit(() -> controller.getAll(accepted));
            assertTrue(admitted.await(5, TimeUnit.SECONDS));
            controller.getAll(rejected);
            verify(rejected).status(503);
            verify(rejected).header("Retry-After", "1");
            verify(rejected, never()).attribute(ApiServlet.DATA_SOURCE);
            verify(source, never()).getConnection();
            release.countDown();
            read.get(5, TimeUnit.SECONDS);
            controller.getAll(accepted);
            assertEquals(1, metrics.counter(MetricRegistry.name(controller.getClass(), "readRejected")).getCount());
        } finally {
            release.countDown();
            worker.shutdownNow();
        }
    }
}
