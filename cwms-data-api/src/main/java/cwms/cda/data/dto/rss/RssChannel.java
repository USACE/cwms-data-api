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

package cwms.cda.data.dto.rss;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;

public final class RssChannel {

    @JsonProperty(required = true)
    @Schema(description = "Title of the RSS channel", required = true)
    private final String title;

    @JacksonXmlProperty(localName = "atom:link")
    @Schema(description = "Atom link for pagination to the next page of RSS items")
    private final AtomLink nextLink;

    @JsonProperty(required = true)
    @Schema(description = "Description of the RSS channel", required = true)
    private final String description;

    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "item")
    @Schema(description = "List of RSS items in the channel")
    private final List<RssItem> items;

    public RssChannel(String title, AtomLink nextLink, String description, List<RssItem> items) {
        this.title = title;
        this.nextLink = nextLink;
        this.description = description;
        this.items = items;
    }

    public String getTitle() {
        return title;
    }

    public AtomLink getNextLink() {
        return nextLink;
    }

    public String getDescription() {
        return description;
    }

    public List<RssItem> getItems() {
        return items;
    }
}
