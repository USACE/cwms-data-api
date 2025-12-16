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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.ZonedDateTime;

public final class RssItem {
    @Schema(description = "Description of the RSS item content")
    private final String description;

    @JsonFormat(
        shape = JsonFormat.Shape.STRING,
        pattern = "EEE, dd MMM yyyy HH:mm:ss zzz",
        locale = "en_US"
    )
    @JacksonXmlProperty(localName = "pubDate")
    @Schema(
        description = "Publication date and time of the RSS item",
        example = "Mon, 15 Dec 2025 12:00:00 EST",
        type = "string",
        format = "date-time"
    )
    private final ZonedDateTime pubDate;

    @JacksonXmlProperty(localName = "guid")
    @Schema(description = "Globally unique identifier for the RSS item")
    private final Guid guid;

    public RssItem(String description, ZonedDateTime pubDate, String guid) {
        this.description = description;
        this.pubDate = pubDate;
        this.guid = new Guid(guid);
    }

    public String getDescription() {
        return description;
    }

    public ZonedDateTime getPubDate() {
        return pubDate;
    }

    public Guid getGuid() {
        return guid;
    }
}
