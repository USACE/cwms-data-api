package cwms.cda.api;

import io.javalin.core.util.Header;
import io.javalin.http.Context;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

public class RangeRequestUtil {

    private RangeRequestUtil() {
        // utility class
    }

    /**
     * Javalin has a method very similar to this in its Context class.  The issue is that Javalin decided to
     * take the InputStream, wrap it in a CompletedFuture and then process the request asynchronously.  This
     * causes problems when the InputStream is tied to a database connection that gets closed before the
     * async processing happens.  This method doesn't do the async thing but tries to support the rest.
     * @param ctx
     * @param is
     * @param mediaType
     * @param totalBytes
     * @throws IOException
     */
    public static void seekableStream(Context ctx, InputStream is, String mediaType, long totalBytes) throws IOException {
        long from = 0;
        long to = totalBytes - 1;
        if (ctx.header(Header.RANGE) == null) {
            ctx.res.setContentType(mediaType);
            // Javalin's version of this method doesn't set the content-length
            // Not setting the content-length makes the servlet container use Transfer-Encoding=chunked.
            // Chunked is a worse experience overall, seems like we should just set the length if we know it.
            writeRange(ctx.res.getOutputStream(), is, from, Math.min(to, totalBytes - 1));
        } else {
            int chunkSize = 128000;
            String rangeHeader = ctx.header(Header.RANGE);
            String[] eqSplit = rangeHeader.split("=", 2);
            String[] dashSplit = eqSplit[1].split("-", -1); // keep empty trailing part

            List<String> requestedRange = Arrays.stream(dashSplit)
                    .filter(s -> !s.isEmpty())
                    .collect(java.util.stream.Collectors.toList());

            from = Long.parseLong(requestedRange.get(0));

            if (from + chunkSize > totalBytes) {
                // chunk bigger than file, write all
                to = totalBytes - 1;
            } else if (requestedRange.size() == 2) {
                // chunk smaller than file, to/from specified
                to = Long.parseLong(requestedRange.get(1));
            } else {
                // chunk smaller than file, to/from not specified
                to = from + chunkSize - 1;
            }

            ctx.status(206);

            ctx.header(Header.ACCEPT_RANGES, "bytes");
            ctx.header(Header.CONTENT_RANGE, "bytes " + from + "-" + to + "/" + totalBytes);

            ctx.res.setContentType(mediaType);
            ctx.header(Header.CONTENT_LENGTH, String.valueOf(Math.min(to - from + 1, totalBytes)));
            writeRange(ctx.res.getOutputStream(), is, from, Math.min(to, totalBytes - 1));
        }
    }


    public static void writeRange(OutputStream out, InputStream in, long from, long to) throws IOException {
        writeRange(out, in, from, to, new byte[8192]);
    }

    public static void writeRange(OutputStream out, InputStream is, long from, long to, byte[] buffer) throws IOException {
        long toSkip = from;
        while (toSkip > 0) {
            long skipped = is.skip(toSkip);
            toSkip -= skipped;
        }

        long bytesLeft = to - from + 1;
        while (bytesLeft != 0L) {
            int maxRead = (int) Math.min(buffer.length, bytesLeft);
            int read = is.read(buffer, 0, maxRead);
            if (read == -1) {
                break;
            }
            out.write(buffer, 0, read);
            bytesLeft -= read;
        }

    }

}
