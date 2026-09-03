package cwms.cda.api;

import com.google.common.flogger.FluentLogger;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.List;
import org.apache.commons.io.IOUtils;

public class RangeRequestUtil {
    static FluentLogger logger = FluentLogger.forEnclosingClass();

    private RangeRequestUtil() {
        // utility class
    }


    public static void seekableStream(Context ctx, InputStream is, String mediaType, long totalBytes) throws IOException {
        seekableStream(ctx, is, 0, mediaType, totalBytes);
    }

    /**
     * This method copies data from InputStream is and into the Context response OutputStream.
     * If the request include a Range request header than the response will be a partial response for the first range.
     * Javalin has a similar method in its Context class that handles the streaming asynchronously.  The Javalin
     * method caused problems when our database connections were closed before the streaming completes.
     * Both SQL Blobs and S3 streams are capable of retrieving Streams at a user-controlled offset.  Those methods
     * should be used and the offset passed in the isPosition parameter.
     * If additional skipping needs to be done, this method will call InputStream.skip() which may have some
     * implications for specific implementations but works efficiently for others.
     * @param ctx the Javalin context
     * @param is the input stream
     * @param isPostion the current position in the input stream.
     * @param mediaType the content type
     * @param totalBytes the total number of bytes in the input stream
     * @throws IOException if either of the streams throw an IOException
     */
    public static void seekableStream(Context ctx, InputStream is, long isPostion, String mediaType, long totalBytes) throws IOException {

        if (ctx.header(Header.RANGE) == null) {
            // Not a range request.
            ctx.res.setContentType(mediaType);

            if(isPostion > 0){
                throw new IllegalArgumentException("Input stream position must be 0 for non-range requests");
            }

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
            }
            requestedRange = RangeParser.interpret(requestedRange, totalBytes);

            long from = requestedRange[0];
            long to = requestedRange[1];

            ctx.status(206);

            ctx.header(Header.ACCEPT_RANGES, "bytes");
            ctx.header(Header.CONTENT_RANGE, "bytes " + from + "-" + to + "/" + totalBytes);

            ctx.res.setContentType(mediaType);
            ctx.header(Header.CONTENT_LENGTH, String.valueOf(Math.min(to - from + 1, totalBytes)));

            if(isPostion < from){
                skip(is, from-isPostion);
            }
            long len = Math.min(to, totalBytes - 1) - from + 1;

            // If the inputOffset to IOUtils.copyLarge is not 0 then IOUtils will do its own skipping.  For reasons
            // that IOUtils explains (quirks of certain streams) it does its skipping via read().  Using read() has
            // performance implications b/c all the skipped data still gets retrieved and copied to memory.  In our
            // use-case the data comes from a database blob/clob/s3.  Copying (potential) gigabytes of data we
            // don't need across the network is not ideal.  We've tried to address this performance impact in two
            // ways.  1) We allow callers to pass an input stream that is already positioned so that skipping isn't
            // needed.  2) If skipping is needed we call InputStream.skip() directly (this is efficient for the
            // stream from Oracle Blobs).

            // We do our own skipping and then have IOUtils copy.
            IOUtils.copyLarge(is, (OutputStream) ctx.res.getOutputStream(), 0, len);
        }
    }

    /**
     * Similar to seekableStream but for Reader. For some reason the java.sql.Clob does not specify a method to get a
     * Stream that is already positioned but there is a method to position a Reader.
     * @param ctx
     * @param reader
     * @param isPostion
     * @param mediaType
     * @param totalBytes
     * @throws IOException
     */
    public static void seekableReader(Context ctx, Reader reader, long isPostion, String mediaType, long totalBytes) throws IOException {

        if (ctx.header(Header.RANGE) == null) {
            // Not a range request.
            ctx.res.setContentType(mediaType);

            if(isPostion > 0){
                throw new IllegalArgumentException("Input stream position must be 0 for non-range requests");
            }

            ctx.header(Header.CONTENT_LENGTH, String.valueOf(totalBytes));

            IOUtils.copyLarge(reader, ctx.res.getWriter(), 0, totalBytes);
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
            }
            requestedRange = RangeParser.interpret(requestedRange, totalBytes);

            long from = requestedRange[0];
            long to = requestedRange[1];

            ctx.status(206);

            ctx.header(Header.ACCEPT_RANGES, "bytes");
            ctx.header(Header.CONTENT_RANGE, "bytes " + from + "-" + to + "/" + totalBytes);

            ctx.res.setContentType(mediaType);
            ctx.header(Header.CONTENT_LENGTH, String.valueOf(Math.min(to - from + 1, totalBytes)));

            if(isPostion < from){
                skip(reader, from-isPostion);
            }
            long len = Math.min(to, totalBytes - 1) - from + 1;

            IOUtils.copyLarge(reader, ctx.res.getWriter(), 0, len);
        }
    }

    private static void skip(InputStream is, long toSkip) throws IOException {
        while (toSkip > 0) {
            long skipped = is.skip(toSkip);
            toSkip -= skipped;
        }
    }

    private static void skip(Reader reader, long toSkip) throws IOException {
        while (toSkip > 0) {
            long skipped = reader.skip(toSkip);
            toSkip -= skipped;
        }
    }

}
