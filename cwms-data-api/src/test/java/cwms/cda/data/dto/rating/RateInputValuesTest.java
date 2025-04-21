/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.rating;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.api.errors.FieldException;
import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

/**
 *
 */
final class RateInputValuesTest {

    @Test
    void testGetValues() {
        List<List<Double>> testValues = Arrays.asList(
            Arrays.asList(1.0, 2.0, 3.0),
            Arrays.asList(4.0, 5.0, 6.0)
        );
        List<String> inputUnits = Arrays.asList("ft", "cfs");
        RateInputValues rateInputValues = new RateInputValues.RateInputValuesBuilder()
            .withValues(testValues)
            .withInputUnits(inputUnits)
            .withOutputUnit("ft")
            .build();
        assertNotNull(rateInputValues.getValues(), "getValues should not return null.");
        assertEquals(testValues, rateInputValues.getValues(), "getValues should return the correct list of values.");
    }

    @Test
    void testGetValueTimes() {
        List<Long> valueTimes = Arrays.asList(Instant.now().minus(5, ChronoUnit.DAYS).toEpochMilli(),
            Instant.now().toEpochMilli());
        List<List<Double>> testValues = Collections.singletonList(Arrays.asList(1.0, 2.0));
        List<String> inputUnits = Collections.singletonList("cfs");
        RateInputValues rateInputValues = new RateInputValues.RateInputValuesBuilder()
            .withValueTimes(valueTimes)
            .withValues(testValues)
            .withInputUnits(inputUnits)
            .withOutputUnit("ft")
            .build();
        assertNotNull(rateInputValues.getValueTimes(), "getValueTimes should not return null.");
        assertEquals(valueTimes, rateInputValues.getValueTimes(),
            "getValueTimes should return the correct list of timestamps.");
    }

    @Test
    void testGetInputUnits() {
        List<String> inputUnits = Arrays.asList("ft", "cfs");
        List<List<Double>> testValues = Arrays.asList(
            Arrays.asList(1.0, 2.0, 3.0),
            Arrays.asList(4.0, 5.0, 6.0)
        );
        RateInputValues rateInputValues = new RateInputValues.RateInputValuesBuilder()
            .withValues(testValues)
            .withInputUnits(inputUnits)
            .withOutputUnit("ft")
            .build();
        assertNotNull(rateInputValues.getInputUnits(), "getInputUnits should not return null.");
        assertEquals(inputUnits, rateInputValues.getInputUnits(),
            "getInputUnits should return the correct list of input units.");
    }

    @Test
    void testBuilderWithInvalidValuesThrowsException() {
        List<List<Double>> invalidValues = null;
        List<String> validUnits = Collections.singletonList("cfs");
        assertThrows(RequiredFieldException.class, () -> {
            new RateInputValues.RateInputValuesBuilder()
                .withValues(invalidValues)
                .withInputUnits(validUnits)
                .withOutputUnit("ft")
                .build()
                .validate();
        }, "Builder should throw an exception when values are null.");
    }

    @Test
    void testBuilderWithEmptyValuesThrowsException() {
        List<List<Double>> invalidValues = new ArrayList<>();
        List<String> validUnits = Collections.singletonList("cfs");
        assertThrows(FieldException.class, () -> {
            new RateInputValues.RateInputValuesBuilder()
                .withValues(invalidValues)
                .withInputUnits(validUnits)
                .withOutputUnit("ft")
                .build()
                .validate();
        }, "Builder should throw an exception when values are empty.");
        invalidValues.add(new ArrayList<>());
        assertThrows(FieldException.class, () -> {
            new RateInputValues.RateInputValuesBuilder()
                .withValues(invalidValues)
                .withInputUnits(validUnits)
                .withOutputUnit("ft")
                .build()
                .validate();
        }, "Builder should throw an exception when values are empty.");
        invalidValues.add(Collections.singletonList(1.0));
        List<String> twoValidUnits = Arrays.asList("cfs", "ft");
        assertThrows(FieldException.class, () -> {
            new RateInputValues.RateInputValuesBuilder()
                .withValues(invalidValues)
                .withInputUnits(twoValidUnits)
                .withOutputUnit("ft")
                .build()
                .validate();
        }, "Builder should throw an exception when values are empty.");
    }

    @Test
    void testBuilderWithMismatchedUnitLengthsThrowsException() {
        List<List<Double>> testValues = Collections.singletonList(Arrays.asList(1.0, 2.0));
        List<String> invalidUnits = Arrays.asList("ft", "cfs");
        assertThrows(FieldException.class, () ->
                new RateInputValues.RateInputValuesBuilder()
                    .withValues(testValues)
                    .withInputUnits(invalidUnits)
                    .withOutputUnit("ft")
                    .build()
                    .validate(),
            "should throw an exception when the number of units don't not match the number of value arrays.");
    }

    @Test
    void testBuilderWithMismatchedValueTimesLengthsThrowsException() {
        List<List<Double>> testValues = Collections.singletonList(Arrays.asList(1.0, 2.0));
        List<String> validUnits = Collections.singletonList("ft");
        List<Long> valueTimes = Arrays.asList(Instant.now().minus(20, ChronoUnit.DAYS).toEpochMilli(),
            Instant.now().minus(5, ChronoUnit.DAYS).toEpochMilli(),
            Instant.now().toEpochMilli());
        assertThrows(FieldException.class, () ->
                new RateInputValues.RateInputValuesBuilder()
                    .withValues(testValues)
                    .withValueTimes(valueTimes)
                    .withInputUnits(validUnits)
                    .withOutputUnit("ft")
                    .build()
                    .validate(),
            "should throw an exception when the number of value times don't not match the number of value arrays.");
    }

    @Test
    void testBuilderWithMismatchedValueArrayLengthsThrowsException() {
        List<List<Double>> testValues = Arrays.asList(Arrays.asList(1.0, 2.0),
            Arrays.asList(1.0, 2.0, 3.0));
        List<String> validUnits = Collections.singletonList("ft");
        assertThrows(FieldException.class, () ->
                new RateInputValues.RateInputValuesBuilder()
                    .withValues(testValues)
                    .withInputUnits(validUnits)
                    .withOutputUnit("ft")
                    .build()
                    .validate(),
            "should throw an exception when the number of value times don't not match the number of value arrays.");
    }

    @Test
    void testBuilderSetsValueTimesCorrectly() {
        List<Long> valueTimes = Arrays.asList(Instant.now().minus(5, ChronoUnit.DAYS).toEpochMilli(),
            Instant.now().toEpochMilli());
        List<List<Double>> testValues = Collections.singletonList(Arrays.asList(1.5, 2.5));
        List<String> inputUnits = Collections.singletonList("cfs");
        RateInputValues rateInputValues = new RateInputValues.RateInputValuesBuilder()
            .withValueTimes(valueTimes)
            .withValues(testValues)
            .withInputUnits(inputUnits)
            .withOutputUnit("ft")
            .build();
        rateInputValues.validate();
        assertNotNull(rateInputValues.getValueTimes(), "ValueTimes should not be null.");
        assertEquals(valueTimes, rateInputValues.getValueTimes(),
            "ValueTimes should match the ones provided in the builder.");
    }

    @Test
    void testSerializationRoundTrip() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/rating/rate_input_values.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        RateInputValues deserialized = Formats.parseContent(contentType, json, RateInputValues.class);
        assertEquals("cfs", deserialized.getOutputUnit(), "Output unit should match.");
        assertEquals(Instant.ofEpochMilli(1672531200000L), deserialized.getRatingTime().get(),
            "Rating time should match.");
        assertTrue(deserialized.getRound(), "Round should match.");
        assertEquals(Arrays.asList("ft", "cfs"), deserialized.getInputUnits(), "Input units should match.");
        assertEquals(Arrays.asList(Arrays.asList(1.0, 2.5, 3.8),
            Arrays.asList(0.5, 1.2, 2.3)), deserialized.getValues(), "Values should match.");
        assertEquals(Arrays.asList(1672531200000L, 1672617600000L, 1672704000000L),
            deserialized.getValueTimes(), "Output unit should match.");

    }
}
