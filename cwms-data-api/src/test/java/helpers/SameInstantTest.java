package helpers;

import static helpers.SameInstantString.sameInstant;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class SameInstantTest {

    @Test
    void testSameInstant() {

        Instant expected = Instant.parse("2021-06-21T08:00:00Z");

        assertAll(
                () -> assertThat("2021-06-21T08:00:00Z", sameInstant("2021-06-21T08:00:00Z")),
                () -> assertThat("2021-06-21T08:00:00Z", sameInstant(expected)),
                () -> assertThat("1624262400000", sameInstant("2021-06-21T08:00:00Z")),
                () -> assertThat("1624262400000", sameInstant("1624262400000")),
                () -> assertThat("1624262400000", sameInstant(1624262400000L))
                // This is meant to be used with RestAssured so we really just want the jackson supported formats.
        );
    }

}