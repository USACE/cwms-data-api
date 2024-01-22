package cwms.cda.api;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.JooqDao;
import io.javalin.http.Context;
import io.javalin.http.sse.SseClient;
import io.javalin.websocket.WsConfig;
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.function.Consumer;
import org.jooq.DSLContext;

public class ClobAsyncController  {
    public static final FluentLogger logger = FluentLogger.forEnclosingClass();

    public Consumer<SseClient> getSseConsumer() {
        return this::accept;
    }

    public Consumer<WsConfig> getWsConsumer() {
        logger.atInfo().log("getWsConsumer");
        return this::acceptWs;
    }

    private void acceptWs(WsConfig ws) {
        logger.atInfo().log("acceptWs");

        ws.onConnect(ctx -> {
            logger.atInfo().log("ws onConnect");

            String clobId = ctx.queryParam("id");
            String officeId= ctx.queryParam("officeId");

            ctx.send("Hello clob Client! ");

            new Thread(() -> sendClob(null, officeId, clobId, reader -> {
                sendToClient(reader, str -> ctx.send( str));
            })).start();

        });
        ws.onClose(ctx -> {
           logger.atInfo().log("ws onClose");
        });
        ws.onMessage(ctx -> {
            logger.atInfo().log("ws onMessage");
        });
        ws.onError(ctx -> {
            logger.atInfo().log("ws onError");
        });

    }

    public void accept(SseClient client) {
        String clobId = client.ctx.queryParam("id");
        String officeId= client.ctx.queryParam("officeId");

        logger.atInfo().log("got an sse clob request for:" + clobId );

        client.sendEvent("retry", "10000\n\n", null);  // could not get this to work...

        client.sendEvent("message", "Hello clob Client! Get ready for your clob!\n");
        client.onClose(() -> logger.atInfo().log("Clob client left."));

        // call sendClob on new thread
        new Thread(() -> sendClob(client.ctx, officeId, clobId, reader -> {
//            client.sendEvent("message", "clob data would go here\n");
            sendToClient(reader, str -> client.sendEvent("message", str));
            client.sendEvent("close", "");  // normally the server just closing the connection would make the browser restart their side.
//            client.close();
        })).start();
    }

    private static void sendToClient( Reader reader, Consumer<String> strConsumer) {
        // read from reader in chunks and send to client
        char[] buffer = new char[32768];
        long total = 0;
        int bytesRead = 0;
        try {
            while (true) {
                if ((bytesRead = reader.read(buffer)) == -1) break;
                total += bytesRead;
                logger.atInfo().log("Sending " + bytesRead + " bytes to client. Total sent: " + total);
                strConsumer.accept(new String(buffer, 0, bytesRead));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    private static void sendClob(Context ctx, String officeId, String clobId, Consumer<Reader> readerConsumer) {
        DSLContext dslContext = JooqDao.getDslContext(ctx);
        dslContext.connection(
                connection -> {
                    String sql = "select cwms_20.AV_CLOB.VALUE from "
                            + "cwms_20.av_clob join cwms_20.av_office on av_clob.office_code = av_office.office_code "
                            + "where av_office.office_id = ? and av_clob.id = ?";
                    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                        preparedStatement.setString(1, officeId);
                        preparedStatement.setString(2, clobId);

                        try (ResultSet resultSet = preparedStatement.executeQuery()) {
                            if (resultSet.next()) {
                                // Get the CLOB column
                                Clob clob = resultSet.getClob("VALUE");
                                if(clob != null) {
                                    // Open a Reader to stream CLOB data
                                    try (Reader reader = clob.getCharacterStream()) {
                                        if (reader != null) {
                                            readerConsumer.accept(reader);
                                        } else {
                                            logger.atInfo().log("clob.getCharacterStream returned null.");
                                        }

                                    }
                                } else {
                                    logger.atInfo().log("clob returned for " + clobId + " was null.");
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.atSevere().withCause(e).log("Error getting clob.");
                        throw new RuntimeException(e);
                    }
                }
        );
    }


    public static String getNowStr() {
        OffsetDateTime currentDateTimeWithOffset = OffsetDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z");
        return currentDateTimeWithOffset.format(formatter);
    }

    private static Optional<String> getUser(Context ctx) {
        Optional<String> retval = Optional.empty();
        if (ctx != null && ctx.req != null && ctx.req.getUserPrincipal() != null) {
            retval = Optional.of(ctx.req.getUserPrincipal().getName());
        } else {
            logger.atFine().log( "No user principal found in request.");
        }
        return retval;
    }

}
