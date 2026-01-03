package cwms.cda.api;

import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.regex.*;

/**
 * Utility class for parsing HTTP Range headers.
 *  These typically look like: bytes=100-1234
 *              or: bytes=100-  this is common to resume a download
 *              or: bytes=0- equivalent to a regular request for the whole file
 *              but by returning 206 we show that we support range requests
 *              Note that multiple ranges can be requested at once such
 *              as: bytes=500-600,700-999 Server responds identifies separator and then puts separator between chunks
 *              bytes=0-0,-1 also legal its just the first and the last byte
 *              or: bytes=500-600,601-999 legal but what is the point?
 *              or: bytes=500-700,601-999 legal, notice they overlap.
 *
 *
 */
public class RangeParser {

    private static final Pattern RANGE_PATTERN = Pattern.compile("(\\d*)-(\\d*)");

    /**
     * Return a list of two element long[] containing byte ranges parsed from the HTTP Range header.
     * If the end of a range is not specified ( e.g. bytes=100- ) then a -1 is returned in the second position
     * If the range only includes a negative byte (e.g bytes=-50) then -1 is returned as the start of the range
     * and -1*end is returned as the end of the range.    bytes=-50 will result in [-1,50]
     *
     * @param header the HTTP Range header this should start with "bytes=" if it is null or empty an empty list is returned
     * @return a list of long[2] holding the ranges
     */
    public static List<long[]> parse(String header) {
        if (header == null || header.isEmpty() ) {
            return Collections.emptyList();
        } else if ( !header.startsWith("bytes=")){
            throw new IllegalArgumentException("Invalid Range header: " + header);
        }

        String rangePart = header.substring(6);
        List<long[]> retval = parseRanges(rangePart);
        if( retval.isEmpty() ){
            throw new IllegalArgumentException("Invalid Range header: " + header);
        }
        return retval;
    }

    public static long[] parseFirstRange(String header) {
        if(header != null) {
            List<long[]> ranges = RangeParser.parse(header);
            if (!ranges.isEmpty()) {
                return ranges.get(0);
            }
        }
        return null;
    }

    public static @NotNull List<long[]> parseRanges(String rangePart) {
        if( rangePart == null || rangePart.isEmpty() ){
            throw new IllegalArgumentException("Invalid range specified: " + rangePart);
        }
        String[] parts = rangePart.split(",");
        List<long[]> ranges = new ArrayList<>();

        for (String part : parts) {
            Matcher m = RANGE_PATTERN.matcher(part.trim());
            if (m.matches()) {
                String start = m.group(1);
                String end = m.group(2);

                long s = start.isEmpty() ? -1 : Long.parseLong(start);
                long e = end.isEmpty() ? -1 : Long.parseLong(end);

                ranges.add(new long[]{s, e});
            }
        }
        return ranges;
    }

    /**
     * The parse() method in this class can return -1 for unspecified values or when suffix ranges are supplied.
     * This method interprets the negative values in regard to the totalSize and returns inclusive indices of the
     * requested range.
     * @param inputs the array of start and end byte positions
     * @param totalBytes the total number of bytes in the file
     * @return a long array with the start and end byte positions, these are inclusive.  [0,0] means return the first byte
     */
    public static long[] interpret(long[] inputs, long totalBytes){
        if(inputs == null){
            throw new IllegalArgumentException("null range array provided");
        } else if( inputs.length != 2 ){
            throw new IllegalArgumentException("Invalid number of inputs: " + Arrays.toString(inputs));
        }

        long start = inputs[0];
        long end = inputs[1];

        if(start == -1L){
            // its a suffix request.
            start = totalBytes - end;
            end = totalBytes - 1;
        } else {
            if (start < 0 ) {
                throw new IllegalArgumentException("Invalid range specified: " + Arrays.toString(inputs));
            }

            if(end == -1L){
                end = totalBytes - 1;
            }

            if(end < start){
                throw new IllegalArgumentException("Invalid range specified: " + Arrays.toString(inputs));
            }

            if(start > totalBytes - 1){
                throw new IllegalArgumentException("Can't satisfy range request: " + Arrays.toString(inputs) + " Range starts beyond end of file.");
            }

            end = Math.min(end, totalBytes - 1);
        }

        return new long[]{start, end};
    }


}
