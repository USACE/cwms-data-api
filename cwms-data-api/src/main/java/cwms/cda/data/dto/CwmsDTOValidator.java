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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.errors.ExclusiveFieldsException;
import cwms.cda.api.errors.FieldException;
import cwms.cda.api.errors.RequiredFieldException;
import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This class is used to validate the fields of a CwmsDTOBase object.
 */
public final class CwmsDTOValidator {

    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final Map<Class<?>, List<Method>> REQUIRED_FIELD_GETTERS = new ConcurrentHashMap<>();
    private final Set<String> missingFields = new HashSet<>();
    private final Set<String> mutuallyExclusiveFields = new HashSet<>();
    private final Set<Exception> validationExceptions = new HashSet<>();

    /**
     * Validates the presence of a required field in a given object or DTO.
     * Adds the field name to a set of missing fields if the value is null.
     * If the value is an instance of CwmsDTOBase, it validates the CwmsDTOBase as well.
     *
     * @param value     the value of the field to be checked
     * @param fieldName the name of the field to be checked
     */
    public void required(Object value, String fieldName) {
        if (value == null) {
            missingFields.add(fieldName);
        } else if (value instanceof CwmsDTOBase) {
            ((CwmsDTOBase) value).validateInternal(this);
        }
    }

    public void validateRequiredFields(CwmsDTOBase cwmsDTO) {
        Class<? extends CwmsDTOBase> type = cwmsDTO.getClass();
        validateFieldsInClass(cwmsDTO, type);
    }

    private void validateFieldsInClass(CwmsDTOBase cwmsDTO, Class<? extends CwmsDTOBase> type) {
        List<Method> getters = REQUIRED_FIELD_GETTERS.computeIfAbsent(type, this::getRequiredFields);
        try {
            for (Method getter : getters) {
                Object value = getter.invoke(cwmsDTO);
                if (value == null) {
                    missingFields.add(getter.getName());
                } else if (value instanceof CwmsDTOBase) {
                    ((CwmsDTOBase) value).validateInternal(this);
                    validateRequiredFields((CwmsDTOBase) value);
                }
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            LOGGER.atWarning().withCause(e).log("Unable to validate required fields are non-null in DTO: " + type);
        }
    }

    private List<Method> getRequiredFields(Class<?> type) {
        List<Method> retval = new ArrayList<>();
        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(type);
            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
            for(PropertyDescriptor propertyDescriptor : propertyDescriptors) {
                Method readMethod = propertyDescriptor.getReadMethod();
                if(readMethod != null) {
                    if (readMethod.getDeclaringClass().getPackage().getName().equals("java.lang")) {
                        continue;
                    }
                    String fieldName = propertyDescriptor.getName();
                    Field field = getDeclaredField(type, fieldName);
                    if(field != null) {
                        JsonProperty annotation = field.getAnnotation(JsonProperty.class);
                        if (annotation != null && annotation.required()) {
                            retval.add(readMethod);
                        }
                    }
                }
            }
        } catch (IntrospectionException e) {
            LOGGER.atWarning().withCause(e)
                .log("Unable to validate required field are non-null in DTO: %s", type);
        }
        return retval;
    }

    private Field getDeclaredField(Class<?> type, String fieldName) {
        Field retval = null;
        if (type != null) {
            try {
                retval = type.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                LOGGER.atFinest().withCause(e)
                    .log("Field does not exist. Will not validate its existence in DTO: %s and field name: %s",
                        type, fieldName);
                retval = getDeclaredField(type.getSuperclass(), fieldName);
            }
        }
        return retval;
    }

    public void validateCollection(Collection<? extends CwmsDTOBase> collection) {
        if (collection != null) {
            collection.forEach(c -> c.validateInternal(this));
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
    public void validate(Callable<?> callable) {
        try {
            callable.call();
        } catch (Exception e) {
            validationExceptions.add(e);
        }
    }
}
