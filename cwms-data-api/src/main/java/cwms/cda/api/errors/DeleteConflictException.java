/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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

package cwms.cda.api.errors;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;

public final class DeleteConflictException extends ApplicationException {

    public DeleteConflictException(String message, SQLException cause) {
        super(message, "Database", "Cannot perform requested delete. "
            + "Data is referenced elsewhere in CWMS.", HttpServletResponse.SC_CONFLICT, new HashMap<>(), cause);
    }

    @Override
    public Map<String, Serializable> getDetails() {
        String sqlExceptionMessage = getCause().getLocalizedMessage();
        String[] parts = sqlExceptionMessage.split("\n");
        if (parts.length > 1) {
            sqlExceptionMessage = parts[0];
        }
        Map<String, Serializable> retval = new HashMap<>();
        retval.put(getMessage(), sqlExceptionMessage);
        return retval;
    }
}
