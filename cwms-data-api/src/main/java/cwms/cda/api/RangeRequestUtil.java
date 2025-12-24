package cwms.cda.api;

import com.google.common.flogger.FluentLogger;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.commons.io.IOUtils;

public class RangeRequestUtil {
    static FluentLogger logger = FluentLogger.forEnclosingClass();

    private RangeRequestUtil() {
        // utility class
    }

    /**
     * Javalin has a method very similar to this in its Context class.  The issue is that Javalin decided to
     * take the InputStream, wrap it in a CompletedFuture and then process the request asynchronously.  This
     * causes problems when the InputStream is tied to a database connection that gets closed before the
     * async processing happens.  This method doesn't do the async thing but tries to support the rest.
     * @param ctx the Javalin context
     * @param is the input stream
     * @param mediaType the content type
     * @param totalBytes the total number of bytes in the input stream
     * @throws IOException if either of the streams throw an IOException
     */
    public static void seekableStream(Context ctx, InputStream is, String mediaType, long totalBytes) throws IOException {

        if (ctx.header(Header.RANGE) == null) {
            // Not a range request.
            ctx.res.setContentType(mediaType);

            // Javalin's version of this method doesn't set the content-length
            // Not setting the content-length makes the servlet container use Transfer-Encoding=chunked.
            // Chunked is a worse experience overall, seems like we should just set the length if we know it.
            ctx.header(Header.CONTENT_LENGTH, String.valueOf(totalBytes));

            IOUtils.copyLarge(is, (OutputStream) ctx.res.getOutputStream(), 0, totalBytes);
        } else {
            String rangeHeader = ctx.header(Header.RANGE);

            List<long[]> ranges = RangeParser.parse(rangeHeader);

            long[] requestedRange = ranges.get(0);
            if( ranges.size() > 1 ){
                // we support range requests but we not currently supporting multiple ranges.
                // Range request are optional so we have choices what to do if multiple ranges are requested:
                // We could return 416 and hope the client figures out to only send one range
                // We could service the first range with 206 and ignore the other ranges
                // We could ignore the range request entirely and return the full body with 200
                // We could implement support for multiple ranges
                logger.atInfo().log("Multiple ranges requested, using first and ignoring additional ranges");
            } else {
                requestedRange = RangeParser.interpret(requestedRange, totalBytes);

                long from = requestedRange[0];
                long to = requestedRange[1];

                ctx.status(206);

                ctx.header(Header.ACCEPT_RANGES, "bytes");
                ctx.header(Header.CONTENT_RANGE, "bytes " + from + "-" + to + "/" + totalBytes);

                ctx.res.setContentType(mediaType);
                ctx.header(Header.CONTENT_LENGTH, String.valueOf(Math.min(to - from + 1, totalBytes)));
                writeRange(ctx.res.getOutputStream(), is, from, Math.min(to, totalBytes - 1));
            }
        }
    }

    /**
     * Writes a range of bytes from the input stream to the output stream.
     * @param out the output stream to write to.
     * @param in the input stream to read from.  It is assumed that this stream is open and positioned at 0.
     * @param from the starting byte position to read from (inclusive)
     * @param to the ending byte position to read to (inclusive)
     * @throws IOException if either of the streams throw an IOException
     */
    public static void writeRange(OutputStream out, InputStream in, long from, long to) throws IOException {
        skip(in, from);
        long len = to - from + 1;

        // If the inputOffset to IOUtils.copyLarge is not 0 then IOUtils will do its own skipping.  For reasons
        // that IOUtils explains (quirks of certain streams) it does its skipping via read().  Using read() has performance
        // implications b/c all the skipped data gets copied to memory.  We do our own skipping and then have IOUtils copy.
        IOUtils.copyLarge(in, out, 0, len);
    }

    private static void skip(InputStream is, long toSkip) throws IOException {
        while (toSkip > 0) {
            long skipped = is.skip(toSkip);
            toSkip -= skipped;
        }
    }

}
