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

package cwms.cda.data.dto.messaging;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.JSON, Formats.DEFAULT})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class CdaTopics extends CwmsDTOBase {
    private List<Map<String, Object>> serverConfigurations = new ArrayList<>();
    private final NavigableSet<String> supportedProtocols = new TreeSet<>();
    private final NavigableSet<String> topics = new TreeSet<>();

    public CdaTopics() {
    }

    public CdaTopics(List<Map<String, Object>> serverConfigurations, Collection<String> supportedProtocols, Collection<String> topics) {
        this.serverConfigurations.addAll(serverConfigurations);
        this.supportedProtocols.addAll(supportedProtocols);
        this.topics.addAll(topics);
    }

    public List<Map<String, Object>> getServerConfigurations() {
        return serverConfigurations;
    }

    public NavigableSet<String> getSupportedProtocols() {
        return this.supportedProtocols;
    }

    public NavigableSet<String> getTopics() {
        return this.topics;
    }

    public void setServerConfigurations(List<Map<String, Object>> serverConfigurations) {
        this.serverConfigurations = serverConfigurations;
    }

    public void setSupportedProtocols(Set<String> supportedProtocols) {

        this.supportedProtocols.addAll(supportedProtocols);
    }

    public void setTopics(Set<String> topics) {
        this.topics.addAll(topics);
    }
}
