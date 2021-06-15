package cwms.radar.data.dto;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.regex.Pattern;

import javax.xml.bind.annotation.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import io.swagger.v3.oas.annotations.media.Schema;

/***
 * Provides a framework for a paginated result set.
 * Implementation details are subject to inheritors.
 * @author Daniel Osborne
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public abstract class CwmsDTOPaginated implements CwmsDTO {
    @Schema(description = "The cursor to the current page of data")
    protected String page;
    @Schema(description = "The cursor to the next page of data; null if there is no more data")
    protected String nextPage;
    @JsonInclude(value = Include.NON_NULL)
    @Schema(description = "The total number of records retrieved; null or not present if not supported or unknown")
    protected Integer total;
    @Schema(description = "The number of records fetched per-page; this may be larger than the number of records actually retrieved")
    protected int pageSize;

    static final Encoder encoder = Base64.getEncoder();
    static final Decoder decoder = Base64.getDecoder();
    static final String delimiter = "||";

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

    protected CwmsDTOPaginated(String page, int pageSize)
    {
        this.pageSize = pageSize;
        this.page = encodeCursor(page, pageSize);
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

    public static String[] decodeCursor(String cursor)
    {
        return decodeCursor(cursor, CwmsDTOPaginated.delimiter);
    }

    public static String[] decodeCursor(String cursor, String delimiter)
    {
        if(cursor != null && !cursor.isEmpty()) {
            String _cursor = new String(decoder.decode(cursor));
            String parts[] = _cursor.split(Pattern.quote(delimiter));
            return parts;
        }

        // Return empty array
        return new String[0];
    }

    public static String encodeCursor(String page, int pageSize)
    {
        return encodeCursor(page, pageSize, CwmsDTOPaginated.delimiter);
    }

    public static String encodeCursor(String page, int pageSize, String delimiter)
    {
        return (page == null || page.equals("*")) ? null : encoder.encodeToString(String.join(delimiter, page, String.format("%d", pageSize)).getBytes());
    }
}
