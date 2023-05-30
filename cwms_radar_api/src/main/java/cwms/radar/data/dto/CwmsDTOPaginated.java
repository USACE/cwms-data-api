package cwms.radar.data.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.logging.Logger;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.xml.bind.annotation.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema.AccessMode;
import kotlin.jvm.functions.Function1;

/***
 * Provides a framework for a paginated result set.
 * Implementation details are subject to inheritors.
 * @author Daniel Osborne
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public abstract class CwmsDTOPaginated implements CwmsDTOBase {
    private static final Logger logger = Logger.getLogger(CwmsDTOPaginated.class.getName());

    @Schema(
        description = "The cursor to the current page of data",
        accessMode = AccessMode.READ_ONLY
    )
    protected String page;

    @Schema(
        description = "The cursor to the next page of data; null if there is no more data",
        accessMode = AccessMode.READ_ONLY
    )
    @XmlElement(name = "next-page")
    @JsonProperty("next-page")
    protected String nextPage;

    @JsonInclude(value = Include.NON_NULL)
    @Schema(        
        description = "The total number of records retrieved; null or not present if not supported or unknown",
        accessMode = AccessMode.READ_ONLY
    )
    protected Integer total;

    @Schema(
        description = "The number of records fetched per-page; this may be larger than the number of records actually retrieved",
        accessMode = AccessMode.READ_ONLY
    )
    @XmlElement(name = "page-size")
    @JsonProperty("page-size")
    protected int pageSize;

    static final Encoder encoder = Base64.getEncoder();
    static final Decoder decoder = Base64.getDecoder();
    public static final String delimiter = "||";

    @SuppressWarnings("unused") // required so JAXB can initialize and marshal
    protected CwmsDTOPaginated() { }

    protected CwmsDTOPaginated(String encodedPage) {
        String[] decoded = decodeCursor(encodedPage);

        // Last item is pageSize
        this.pageSize = Integer.parseInt(decoded[decoded.length - 1]);

        // Build a new list and strip off the pageSize
        List<String> list = Arrays.asList(decoded);
        list.remove(list.size() - 1);

        // Build a new cursor/page marker
        this.page = encodeCursor(String.join(delimiter, list), pageSize);
    }

    protected CwmsDTOPaginated(String page, int pageSize) {
        this.pageSize = pageSize;
        this.page = encodeCursor(page, pageSize);
    }

    protected CwmsDTOPaginated(String page, int pageSize, Integer total) {
        this.pageSize = pageSize;
        this.total = total;
        this.page = encodeCursor(page, pageSize, total);
    }

    /**
     * @return String return the page
     */
    public String getPage() {
        return page;
    }

    /**
     * @return String return the nextPage
     */
    public String getNextPage() {
        return nextPage;
    }

    /**
     * @return Integer return the total, null if not supported or unknown
     */
    public Integer getTotal() {
        return total;
    }

    /**
     * @return int return the elements per page
     */
    public int getPageSize() {
        return pageSize;
    }

    public static String[] decodeCursor(String cursor) {
        return decodeCursor(cursor, CwmsDTOPaginated.delimiter);
    }

    public static String[] decodeCursor(String cursor, String delimiter) {
        if(cursor != null && !cursor.isEmpty()) {
            String _cursor = new String(decoder.decode(cursor));
            return _cursor.split(Pattern.quote(delimiter));
        }

        // Return empty array
        return new String[0];
    }

    public static String encodeCursor(String page, int pageSize) {
        return encodeCursor(CwmsDTOPaginated.delimiter, page, pageSize);
    }

    public static String encodeCursor(String page, int pageSize, Integer total) {
        return encodeCursor(CwmsDTOPaginated.delimiter, page, total, pageSize);     // pageSize should be last
    }

    public static String encodeCursor(Object ... parts) {
        return encodeCursor(CwmsDTOPaginated.delimiter, parts);
    }

    public static String encodeCursor(String delimiter, Object ... parts)
    {
        return (parts.length == 0 || parts[0] == null || parts[0].equals("*")) ? null :
            encoder.encodeToString(Arrays.stream(parts).map(String::valueOf).collect(Collectors.joining(delimiter)).getBytes());
    }

    public static class CursorCheck implements Function1<String,Boolean> {
        private static Pattern base64 = Pattern.compile("^[-A-Za-z0-9+/]*={0,3}$");
        @Override
        public Boolean invoke(String cursor) {
            logger.finest("checking");
            return base64.matcher(cursor).matches() ? Boolean.TRUE : Boolean.FALSE;
        }

    }

    public static CursorCheck CURSOR_CHECK = new CursorCheck();
}
