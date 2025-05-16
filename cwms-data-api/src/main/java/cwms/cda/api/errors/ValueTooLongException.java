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

public final class ValueTooLongException extends RuntimeException {
	private String parameter;
	private int length;
	private int maxLength;
	private boolean suppressIncidentId = true;

	public ValueTooLongException(String message) {
		super(message);
	}

	public ValueTooLongException(String message, Throwable cause) {
		super(message, cause);
	}

	public ValueTooLongException(String message, String parameter, int length, int maxLength, Throwable cause) {
		super(message, cause);
		this.parameter = parameter;
		this.length = length;
		this.maxLength = maxLength;
	}

	public ValueTooLongException(String message, String parameter, int length, int maxLength, Throwable cause, boolean suppressIncidentId) {
		super(message, cause);
		this.parameter = parameter;
		this.length = length;
		this.maxLength = maxLength;
		this.suppressIncidentId = suppressIncidentId;
	}

	public boolean hasParameter()
	{
		return parameter != null && !parameter.isEmpty() && length > 0 && maxLength > 0;
	}

	public static ValueTooLongException fromString(String messageToParse, String message, Throwable cause, boolean suppressIncidentId) {
		String[] parts = messageToParse.split("\"");
		String parameter = parts[parts.length - 2];
		String lengthString = parts[parts.length - 1];
		int actualLength = Integer.parseInt(lengthString.split("actual:")[1].split(",")[0].trim());
		int maxLength = Integer.parseInt(lengthString.split("maximum:")[1].split("\\)")[0].trim());
		return new ValueTooLongException(message, parameter, actualLength, maxLength, cause, suppressIncidentId);
	}

	public static ValueTooLongException fromString(String messageToParse, String message, Throwable cause) {
		return fromString(messageToParse, message, cause, true);
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
