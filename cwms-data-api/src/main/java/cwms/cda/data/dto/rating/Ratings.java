package cwms.cda.data.dto.rating;

import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.OutputFormatter;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.xml.XMLv2;

@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.DEFAULT, Formats.XML})
@FormattableWith(contentType = Formats.JSONV2, formatter = OutputFormatter.DUMMY.class, aliases = {Formats.JSON})
public class Ratings extends CwmsDTOBase {
    /** marker interface for ContentType Annotations */
}
