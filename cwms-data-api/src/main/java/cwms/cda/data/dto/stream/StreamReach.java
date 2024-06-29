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

package cwms.cda.data.dto.stream;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = StreamReach.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class StreamReach implements CwmsDTOBase {

    private final String comment;
    private final StreamLocationNode downstreamNode;
    private final StreamLocationNode upstreamNode;
    private final CwmsId configurationId;
    private final CwmsId streamId;
    private final CwmsId id;

    private StreamReach(Builder builder) {
        this.comment = builder.comment;
        this.downstreamNode = builder.downstreamNode;
        this.upstreamNode = builder.upstreamNode;
        this.configurationId = builder.configurationId;
        this.streamId = builder.streamId;
        this.id = builder.id;
    }

    @Override
    public void validate() throws FieldException {
        if (this.id == null) {
            throw new FieldException("The 'id' field of a StreamReach cannot be null.");
        }
        id.validate();
        if (this.streamId == null) {
            throw new FieldException("The 'streamId' field of a StreamReach cannot be null or empty.");
        }
        streamId.validate();
        if (this.upstreamNode == null) {
            throw new FieldException("The 'upstreamNode' field of a StreamReach cannot be null or empty.");
        }
        upstreamNode.validate();
        if (this.downstreamNode == null) {
            throw new FieldException("The 'downstreamNode' field of a StreamReach cannot be null or empty.");
        }
        downstreamNode.validate();
        if (this.configurationId == null) {
            throw new FieldException("The 'configurationId' field of a StreamReach cannot be null or empty.");
        }
    }

    public String getComment() {
        return comment;
    }

    public StreamLocationNode getDownstreamNode() {
        return downstreamNode;
    }

    public StreamLocationNode getUpstreamNode() {
        return upstreamNode;
    }

    public CwmsId getConfigurationId() {
        return configurationId;
    }

    public CwmsId getStreamId() {
        return streamId;
    }

    public CwmsId getId() {
        return id;
    }

    public static class Builder {
        private String comment;
        private StreamLocationNode downstreamNode;
        private StreamLocationNode upstreamNode;
        private CwmsId configurationId;
        private CwmsId streamId;
        private CwmsId id;

        public Builder withComment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder withDownstreamNode(StreamLocationNode downstreamNode) {
            this.downstreamNode = downstreamNode;
            return this;
        }

        public Builder withUpstreamNode(StreamLocationNode upstreamNode) {
            this.upstreamNode = upstreamNode;
            return this;
        }

        public Builder withConfigurationId(CwmsId configurationId) {
            this.configurationId = configurationId;
            return this;
        }

        public Builder withStreamId(CwmsId streamId) {
            this.streamId = streamId;
            return this;
        }

        public Builder withId(CwmsId id) {
            this.id = id;
            return this;
        }

        public StreamReach build() {
            return new StreamReach(this);
        }
    }
}