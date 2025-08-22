package helpers;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * A matcher that tests that an examined float is a number equal to a value within some range of
 * acceptable error.
 * RestAssured returns Floats from JSON paths and the hamcrest closeTo matcher only works with
 * doubles. This matcher works just like the Hamcrest matcher but operates on float inputs.
 * A slightly different name (floatCloseTo) is used to avoid confusion with the Hamcrest closeTo.
 * <p/>
 * For example:
 * <pre>assertThat(1.03f, is(floatCloseTo(1.0, 0.03)))</pre>
 */
public class FloatCloseTo extends TypeSafeMatcher<Float> {
    private final double delta;
    private final double value;

        public FloatCloseTo(double value, double error) {
            this.delta = error;
            this.value = value;
        }

        @Override
        public boolean matchesSafely(Float item) {
            return actualDelta(item) <= 0.0;
        }

        @Override
        public void describeMismatchSafely(Float item, Description mismatchDescription) {
            mismatchDescription.appendValue(item)
                    .appendText(" differed by ")
                    .appendValue(actualDelta(item));
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("a numeric value within ")
                    .appendValue(delta)
                    .appendText(" of ")
                    .appendValue(value);
        }

        private double actualDelta(Float item) {
            return (Math.abs((item - value)) - delta);
        }

        /**
         * Creates a matcher of {@link Float}s that matches when an examined float is equal
         * to the specified <code>operand</code>, within a range of +/- <code>error</code>.
         * <p/>
         * For example:
         * <pre>assertThat(1.03f, is(floatCloseTo(1.0, 0.03)))</pre>
         *
         * @param operand
         *     the expected value of matching doubles
         * @param error
         *     the delta (+/-) within which matches will be allowed
         */
        @Factory
        public static Matcher<Float> floatCloseTo(double operand, double error) {
            return new FloatCloseTo(operand, error);
        }


}
