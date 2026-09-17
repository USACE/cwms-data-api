package cwms.cda.api;

import java.util.concurrent.Semaphore;

/** No-queue admission: permits remain owned until the read and response have actually finished. */
final class TimeSeriesReadAdmission {
    private final Semaphore permits;

    TimeSeriesReadAdmission(int maximum) {
        if (maximum < 1 || maximum > 64) {
            throw new IllegalArgumentException("Time-series read concurrency must be between 1 and 64");
        }
        permits = new Semaphore(maximum);
    }

    boolean acquire() {
        return permits.tryAcquire();
    }

    void release() {
        permits.release();
    }
}
