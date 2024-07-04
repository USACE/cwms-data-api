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

package cwms.cda.data.dto.project;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import java.util.Collections;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * This class holds a project and lists of the project child locations by kind.
 */
@JsonDeserialize(builder = ProjectChildLocations.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, aliases = {Formats.JSON}, formatter = JsonV2.class)
public class ProjectChildLocations implements CwmsDTOBase {

    private final CwmsId project;

    private final List<CwmsId> embankments;
    private final List<CwmsId> locks;
    private final List<CwmsId> outlets;
    private final List<CwmsId> turbines;
    private final List<CwmsId> gates;

    private ProjectChildLocations(Builder builder) {
        this.project = builder.project;
        this.embankments = builder.embankments;
        this.locks = builder.locks;
        this.outlets = builder.outlets;
        this.turbines = builder.turbines;
        this.gates = builder.gates;
    }

    @Override
    public void validate() throws FieldException {

    }

    public CwmsId getProject() {
        return project;
    }

    public List<CwmsId> getEmbankments() {
        return embankments;
    }

    public List<CwmsId> getLocks() {
        return locks;
    }

    public List<CwmsId> getOutlets() {
        return outlets;
    }

    public List<CwmsId> getTurbines() {
        return turbines;
    }

    public List<CwmsId> getGates() {
        return gates;
    }


    public static class Builder {
        private CwmsId project;
        private List<CwmsId> embankments;
        private List<CwmsId> locks;
        private List<CwmsId> outlets;
        private List<CwmsId> turbines;
        private List<CwmsId> gates;

        public Builder withProject(CwmsId project) {
            this.project = project;
            return this;
        }

        public Builder withEmbankments(List<CwmsId> embankments) {
            this.embankments = wrapList(embankments);
            return this;
        }


        public Builder withLocks(List<CwmsId> locks) {
            this.locks = wrapList(locks);
            return this;
        }

        public Builder withOutlets(List<CwmsId> outlets) {
            this.outlets = outlets;
            return this;
        }

        public Builder withTurbines(List<CwmsId> turbines) {
            this.turbines = wrapList(turbines);
            return this;
        }

        public Builder withGates(List<CwmsId> gates) {
            this.gates = wrapList(gates);
            return this;
        }

        @Nullable
        private static List<CwmsId> wrapList(@Nullable List<CwmsId> embankments) {
            List<CwmsId> retval = null;
            if (embankments != null) {
                retval = Collections.unmodifiableList(embankments);
            }
            return retval;
        }

        public ProjectChildLocations build() {
            return new ProjectChildLocations(this);
        }
    }
}
