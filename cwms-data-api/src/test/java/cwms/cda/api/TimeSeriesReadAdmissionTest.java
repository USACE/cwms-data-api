package cwms.cda.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TimeSeriesReadAdmissionTest {
    @Test
    void bulkLeaseIsIdempotentAndOnlyReleasesItsOwnPermit() {
        TimeSeriesReadAdmission admission = new TimeSeriesReadAdmission(2);
        TimeSeriesReadAdmission.Lease first = admission.lease();
        TimeSeriesReadAdmission.Lease second = admission.lease();
        final TimeSeriesReadAdmission.Lease rejected = admission.lease();
        first.acquire();
        first.acquire();
        second.acquire();
        assertThrows(TimeSeriesReadAdmission.CapacityExceeded.class, rejected::acquire);
        rejected.close();
        assertFalse(admission.acquire());
        first.close();
        first.close();
        assertTrue(admission.acquire());
        assertFalse(admission.acquire());
        admission.release();
        second.close();
        assertThrows(IllegalStateException.class, first::acquire);
        assertTrue(admission.acquire());
        assertTrue(admission.acquire());
        assertFalse(admission.acquire());
    }

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
