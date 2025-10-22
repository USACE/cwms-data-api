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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ApplicationException extends RuntimeException {
    private final String source;
    private final Map<String, Serializable> details;
    private final String cdaErrorMessage;
    private final int cdaHttpErrorCode;

    /**
     * Constructs a new ApplicationException with the specified details.
     * This is a base exception class for errors that inherit from RuntimeException.
     *
     * @param message The error message.
     * @param source The source of the error.
     * @param cdaErrorMessage The CDA error message.
     * @param details Additional details about the error.
     * @param cause The cause of the error.
     */
    public ApplicationException(String message, String source, String cdaErrorMessage, int cdaHttpErrorCode,
            Map<String, Serializable> details, Throwable cause) {
        super(message, cause);
        this.source = source;
        this.details = details;
        this.cdaHttpErrorCode = cdaHttpErrorCode;
        this.cdaErrorMessage = cdaErrorMessage;
    }

    public String getSource() {
        return source;
    }

    public Map<String, Serializable> getDetails() {
        return Collections.unmodifiableMap(details);
    }

    public String getCdaErrorMessage() {
        return cdaErrorMessage;
    }

    public int getCdaHttpErrorCode() {
        return cdaHttpErrorCode;
    }

    static Map<String, Serializable> buildDetailsMap(String message) {
        Map<String, Serializable> details = new HashMap<>();
        details.put("message", message);
        return details;
    }
}
