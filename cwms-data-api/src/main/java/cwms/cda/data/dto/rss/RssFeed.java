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

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.xml.XMLv2;
import io.swagger.v3.oas.annotations.media.Schema;

@FormattableWith(contentType = Formats.RSS, formatter = XMLv2.class, aliases = {Formats.DEFAULT, Formats.XML})
@JacksonXmlRootElement(localName = "rss")
public class RssFeed extends CwmsDTOBase {

    @JacksonXmlProperty(isAttribute = true, localName = "version")
    private final String version = "2.0";

    @JacksonXmlProperty(isAttribute = true, localName = "xmlns:atom")
    private final String atomNs = "http://www.w3.org/2005/Atom";

    @Schema(description = "The RSS channel containing feed metadata and items")
    @JacksonXmlProperty(localName = "channel")
    private final RssChannel channel;

    public RssFeed(RssChannel channel) {
        this.channel = channel;
    }

    public String getVersion() {
        return version;
    }

    public RssChannel getChannel() {
        return channel;
    }
}
