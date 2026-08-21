/*
 *
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api.errors;

import java.util.HashMap;
import java.util.logging.Level;
import javax.servlet.http.HttpServletResponse;

public final class FieldLengthExceededException extends ApplicationException {
    private static final Level LOG_LEVEL = Level.INFO;
    private String parameter;
    private int length;
    private int maxLength;
    private boolean suppressIncidentId = true;

    private static final String DEFAULT_ERROR
        = "One or more provided values exceeds the maximum length for the parameter.";

    public FieldLengthExceededException(Throwable cause) {
        super(DEFAULT_ERROR, USER_INPUT_SOURCE, DEFAULT_ERROR, HttpServletResponse.SC_BAD_REQUEST,
            LOG_LEVEL, new HashMap<>(), cause);
    }

    /**
     * Constructor for FieldLengthExceededException
     * @param parameter the name of the parameter that is too long
     * @param length the length of the provided value
     * @param maxLength the maximum allowed length for the parameter
     * @param cause the underlying cause of the exception
     * @param suppressIncidentId flag to indicate whether to suppress the incident ID in the error response
     */
    public FieldLengthExceededException(String parameter, int length, int maxLength,
            Throwable cause, boolean suppressIncidentId) {
        super(String.format("%s The field %s with provided length of %d "
                + "has a maximum length of %d characters.", DEFAULT_ERROR, parameter, length, maxLength),
            USER_INPUT_SOURCE, String.format("%s The field %s with provided length of %d "
                + "has a maximum length of %d characters.", DEFAULT_ERROR, parameter, length, maxLength),
            HttpServletResponse.SC_BAD_REQUEST, LOG_LEVEL, new HashMap<>(), cause);
        this.parameter = parameter;
        this.length = length;
        this.maxLength = maxLength;
        this.suppressIncidentId = suppressIncidentId;
    }

    public boolean hasParameter() {
        return parameter != null && !parameter.isEmpty() && length > 0 && maxLength > 0;
    }

    public String getParameter() {
        return parameter;
    }

    public int getLength() {
        return length;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public boolean isSuppressIncidentId() {
        return suppressIncidentId;
    }
}
