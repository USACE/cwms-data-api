package cwms.cda.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TimeSeriesReadAdmissionTest {
    @Test
    void burstCannotExceedLimitAndRecoversAfterOwnersRelease() throws Exception {
        TimeSeriesReadAdmission admission = new TimeSeriesReadAdmission(8);
        AtomicInteger admitted = new AtomicInteger();
        CountDownLatch attempts = new CountDownLatch(1000);
        ExecutorService callers = Executors.newFixedThreadPool(32);
        try {
            for (int i = 0; i < 1000; i++) {
                callers.execute(() -> {
                    if (admission.acquire()) {
                        admitted.incrementAndGet();
                    }
                    attempts.countDown();
                });
            }
            assertTrue(attempts.await(5, TimeUnit.SECONDS));
            assertEquals(8, admitted.get());
            assertFalse(admission.acquire());
            for (int i = 0; i < 8; i++) {
                admission.release();
            }
            assertTrue(admission.acquire());
            admission.release();
        } finally {
            callers.shutdownNow();
        }
    }
}
