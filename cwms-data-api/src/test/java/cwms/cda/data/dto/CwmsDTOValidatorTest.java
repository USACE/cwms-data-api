/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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

package cwms.cda.data.dto;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;

import com.fasterxml.jackson.annotation.JsonProperty;
import cwms.cda.api.errors.FieldException;
import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

final class CwmsDTOValidatorTest {

    @Test
    void testLargeCwmsDTOValidationOneRequiredField() {
        List<OneField> collect = IntStream.range(0, 100_000)
            .mapToObj(i -> new OneField("name" + i))
            .collect(toList());

        CwmsDTOHolder cwmsDTOHolder = new CwmsDTOHolder(collect);
        assertTimeout(Duration.ofMillis(150L), cwmsDTOHolder::validate);
    }

    @Test
    void testLargeCwmsDTOValidationNineRequiredFieldValid() {
        List<NineFields> collect = IntStream.range(0, 100_000)
            .mapToObj(i -> new NineFieldsFilled("name" + i, "_"))
            .collect(toList());

        CwmsDTOHolder cwmsDTOHolder = new CwmsDTOHolder(collect);
        assertTimeout(Duration.ofMillis(150L), cwmsDTOHolder::validate);
    }

    @Test
    void testLargeCwmsDTOValidationNineRequiredFieldInvalid() {
        List<NineFields> collect = IntStream.range(0, 100_000)
            .mapToObj(i -> new NineFields("name" + i))
            .collect(toList());

        CwmsDTOHolder cwmsDTOHolder = new CwmsDTOHolder(collect);
        assertTimeout(Duration.ofMillis(150L), () -> assertThrows(FieldException.class, cwmsDTOHolder::validate));
    }

    private static final class OneField extends CwmsDTOBase {
        @JsonProperty(required = true)
        private String name;
        private String name1;
        private String name2;
        private String name3;
        private String name4;
        private String name5;
        private String name6;
        private String name7;
        private String name8;

        private OneField(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public String getName1() {
            return name1;
        }

        public String getName2() {
            return name2;
        }

        public String getName3() {
            return name3;
        }

        public String getName4() {
            return name4;
        }

        public String getName5() {
            return name5;
        }

        public String getName6() {
            return name6;
        }

        public String getName7() {
            return name7;
        }

        public String getName8() {
            return name8;
        }
    }

    private static class NineFields extends CwmsDTOBase {
        @JsonProperty(required = true)
        String name;
        @JsonProperty(required = true)
        String name1;
        @JsonProperty(required = true)
        String name2;
        @JsonProperty(required = true)
        String name3;
        @JsonProperty(required = true)
        String name4;
        @JsonProperty(required = true)
        String name5;
        @JsonProperty(required = true)
        String name6;
        @JsonProperty(required = true)
        String name7;
        @JsonProperty(required = true)
        String name8;

        private NineFields(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public String getName1() {
            return name1;
        }

        public String getName2() {
            return name2;
        }

        public String getName3() {
            return name3;
        }

        public String getName4() {
            return name4;
        }

        public String getName5() {
            return name5;
        }

        public String getName6() {
            return name6;
        }

        public String getName7() {
            return name7;
        }

        public String getName8() {
            return name8;
        }

    }


    private static final class NineFieldsFilled extends NineFields{


        private NineFieldsFilled(String name, String suffix) {
            super(name + suffix);
            this.name1 = name + suffix;
            this.name2 = name + suffix;
            this.name3 = name + suffix;
            this.name4 = name + suffix;
            this.name5 = name + suffix;
            this.name6 = name + suffix;
            this.name7 = name + suffix;
            this.name8 = name + suffix;
        }
    }

    private static final class CwmsDTOHolder extends CwmsDTOBase {
        @JsonProperty(required = true)
        private final CwmsId cwmsId;
        private List<? extends CwmsDTOBase> objects;

        private CwmsDTOHolder(List<? extends CwmsDTOBase> objects) {
            this.cwmsId = new CwmsId.Builder()
                .withOfficeId("SPK")
                .withName("Test")
                .build();
            this.objects = objects;
        }

        public List<? extends CwmsDTOBase> getObjects() {
            return this.objects;
        }

        @Override
        protected void validateInternal(CwmsDTOValidator validator) {
            super.validateInternal(validator);
            validator.validateCollection(objects);
        }
    }

}
