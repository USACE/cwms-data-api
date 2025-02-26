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
package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@JsonDeserialize(builder = TypedTimeSeriesIdentifiersList.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
public class TypedTimeSeriesIdentifiersList extends CwmsDTOPaginated {

    private final List<TypedTimeSeriesIdentifiers> typedTimeSeriesIdentifiers;
    private final int offset;

    private TypedTimeSeriesIdentifiersList(int offset, int pageSize, Integer total, List<TypedTimeSeriesIdentifiers> identifiersList) {
        super(Integer.toString(offset), pageSize, total);
        this.typedTimeSeriesIdentifiers = new ArrayList<>(identifiersList);
        this.offset = offset;
    }

    public List<TypedTimeSeriesIdentifiers> getTypedTimeSeriesIdentifiers() {
        return Collections.unmodifiableList(typedTimeSeriesIdentifiers);
    }

    public static class Builder {
        private final int offset;
        private final int pageSize;
        private final Integer total;

        private List<TypedTimeSeriesIdentifiers> typedTimeSeriesIdentifiers = new ArrayList<>();

        public Builder(int offset, int pageSize, Integer total) {
            this.offset = offset;
            this.pageSize = pageSize;
            this.total = total;
        }

        public Builder withTypedTimeSeriesIdentifiers(Collection<TypedTimeSeriesIdentifiers> identifiersList) {
            this.typedTimeSeriesIdentifiers = new ArrayList<>(identifiersList);
            return this;
        }

        public TypedTimeSeriesIdentifiersList build() {
            TypedTimeSeriesIdentifiersList retval = new TypedTimeSeriesIdentifiersList(offset, pageSize, total, typedTimeSeriesIdentifiers);

            if (this.typedTimeSeriesIdentifiers.size() == this.pageSize) {
                String cursor = Integer.toString(retval.offset + retval.typedTimeSeriesIdentifiers.size());
                retval.nextPage = encodeCursor(cursor, retval.pageSize, retval.total);
            } else {
                retval.nextPage = null;
            }
            return retval;
        }
    }
}
