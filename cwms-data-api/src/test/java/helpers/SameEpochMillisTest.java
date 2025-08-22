package helpers;

import static helpers.SameEpochMillis.assertSameEpoch;
import static helpers.SameEpochMillis.sameEpochMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class SameEpochMillisTest {
    @Test
    void testSameEpochMillis() {

        Instant expected = Instant.parse("2021-06-21T08:00:00Z");

        assertAll(
                () -> assertThat(1624262400000L, sameEpochMillis("2021-06-21T08:00:00Z")),
                () -> assertThat(1624262400000L, sameEpochMillis(expected)),
                () -> assertThat(1624262400000L, sameEpochMillis("2021-06-21T08:00:00Z")),
                () -> assertThat(1624262400000L, sameEpochMillis("1624262400000")),
                () -> assertThat(1624262400000L, sameEpochMillis(1624262400000L))
                // This is meant to be used with RestAssured so we really just want the jackson supported formats.
        );

        assertSameEpoch("2021-06-21T08:00:00Z",1624262400000L, "Should be the same" );
    }
}