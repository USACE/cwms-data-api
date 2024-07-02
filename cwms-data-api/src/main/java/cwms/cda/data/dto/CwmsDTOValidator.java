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

import cwms.cda.api.errors.ExclusiveFieldsException;
import cwms.cda.api.errors.FieldException;
import cwms.cda.api.errors.RequiredFieldException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;


/**
 * This class is used to validate the fields of a CwmsDTO object.
 */
public final class CwmsDTOValidator {

    private final Set<String> missingFields = new HashSet<>();
    private final Set<String> mutuallyExclusiveFields = new HashSet<>();
    private final Set<Exception> validationExceptions = new HashSet<>();

    /**
     * Validates the presence of a required field in a given object or DTO.
     * Adds the field name to a set of missing fields if the value is null.
     * If the value is an instance of CwmsDTO, it validates the CwmsDTO as well.
     *
     * @param value     the value of the field to be checked
     * @param fieldName the name of the field to be checked
     */
    public void required(Object value, String fieldName) {
        if (value == null) {
            missingFields.add(fieldName);
        } else if (value instanceof CwmsDTO) {
            ((CwmsDTO) value).validateInternal(this);
        }
    }

    void validate() throws RequiredFieldException {
        if (!missingFields.isEmpty()) {
            throw new RequiredFieldException(missingFields);
        }
        if (!mutuallyExclusiveFields.isEmpty()) {
            throw new ExclusiveFieldsException(mutuallyExclusiveFields);
        }
        if (!validationExceptions.isEmpty()) {
            FieldException ex = new FieldException("Invalid CWMS DTO provided");
            validationExceptions.forEach(ex::addSuppressed);
            throw ex;
        }
    }

    /**
     * Checks if the given fields are mutually exclusive and adds a message to the list of mutually exclusive fields if they are not.
     *
     * @param message         the message to be added to the list of mutually exclusive fields if the fields are not mutually exclusive
     * @param exclusiveFields the fields to be checked for mutual exclusivity
     */
    public void mutuallyExclusive(String message, Object... exclusiveFields) {
        long count = Arrays.stream(exclusiveFields).filter(Objects::nonNull).count();
        if (count > 1) {
            mutuallyExclusiveFields.add(message);
        }
    }

    /**
     * Validates a given Callable by executing it and capturing any exceptions thrown.
     *
     * @param callable the Callable to be validated
     */
    public void validate(Callable callable) {
        try {
            callable.call();
        } catch (Exception e) {
            validationExceptions.add(e);
        }
    }
}
