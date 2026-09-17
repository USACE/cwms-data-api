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

    Lease lease() {
        return new Lease();
    }

    /** One synchronous request owns this lease through database work and response writing. */
    final class Lease implements AutoCloseable {
        private boolean held;
        private boolean closed;

        void acquire() {
            if (closed) {
                throw new IllegalStateException("Read capacity lease is already closed");
            }
            if (!held) {
                if (!TimeSeriesReadAdmission.this.acquire()) {
                    throw new CapacityExceeded();
                }
                held = true;
            }
        }

        @Override
        public void close() {
            if (held) {
                TimeSeriesReadAdmission.this.release();
                held = false;
            }
            closed = true;
        }
    }

    static final class CapacityExceeded extends RuntimeException {
        private CapacityExceeded() {
            super("Time-series bulk-read capacity is busy; narrow the date range or retry with backoff.");
        }
    }
}
