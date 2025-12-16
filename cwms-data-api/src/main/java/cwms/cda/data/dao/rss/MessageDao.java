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

package cwms.cda.data.dao.rss;

import static java.util.stream.Collectors.toList;
import static org.jooq.impl.DSL.currentTimestamp;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.partitionBy;
import static org.jooq.impl.DSL.rowNumber;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.table;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.rss.AtomLink;
import cwms.cda.data.dto.rss.RssChannel;
import cwms.cda.data.dto.rss.RssFeed;
import cwms.cda.data.dto.rss.RssItem;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;

public final class MessageDao extends JooqDao<RssFeed> {


    public MessageDao(DSLContext dsl) {
        super(dsl);
    }

    public RssFeed retrieveFeed(String cursor, int pageSize, String office, String name,
        Instant since, UnaryOperator<String> urlBuilder) {
        AqTable aqTable = getAqTable(name);
        String[] cursorSplit = CwmsDTOPaginated.decodeCursor(cursor);
        int offset = 0;
        if(cursorSplit.length == 2) {
            offset = Integer.parseInt(cursorSplit[0]);
            pageSize = Integer.parseInt(cursorSplit[1]);
            since = null;
        }
        var items = retrieveMessages(offset, pageSize, since, office, aqTable)
            .map(record -> {
                Object userData = record.get("USER_DATA");
                return MessageUtil.extractPayload(userData).map(p -> rssItem(record, p));
            }).stream()
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(toList());
        AtomLink nextLink = null;
        if(items.size() == pageSize) {
            String nextCursor = CwmsDTOPaginated.encodeCursor(items.size() + offset, pageSize);
            nextLink = new AtomLink("next", urlBuilder.apply(nextCursor));
        }
        String description;
        switch(aqTable) {
            case TS_STORED:
                description = " CWMS messages about time series operations, such as data stored and deleted";
                break;
            case STATUS:
                description = " CWMS general system and application status messages";
                break;
            case REALTIME_OPS:
                description = " CWMS application operational messages";
                break;
            default:
                description = null;
        }
        RssChannel channel = new RssChannel(office + "_" + name, nextLink, description, items);
        return new RssFeed(channel);
    }

    private static AqTable getAqTable(String name) {
        try {
            return AqTable.valueOf(name.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new NotFoundException(e);
        }
    }

    private static RssItem rssItem(Record record, String p) {
        ZonedDateTime enqTimestamp = record.get("ENQ_TIMESTAMP", Timestamp.class)
            .toInstant().atZone(ZoneOffset.UTC);
        String msgId = record.get("MSG_ID", String.class);
        return new RssItem(p, enqTimestamp, msgId);
    }

    private Result<?> retrieveMessages(int offset, int pageSize, Instant since, String office, AqTable name) {
        Timestamp sinceTimestamp = since == null ? null : Timestamp.from(since);
        Table<?> t = table(name("CWMS_20", "AQ$" + office + "_" + name.name() + "_TABLE")).as("t");

        Field<String> MSG_ID         = field("MSG_ID", String.class);
        Field<Timestamp> ENQ_TS      = field("ENQ_TIMESTAMP", Timestamp.class);
        Field<Object> USER_DATA      = field("USER_DATA", Object.class);
        Field<Integer> rn = rowNumber()
                .over(partitionBy(MSG_ID).orderBy(ENQ_TS.desc()))
                .as("rn");
        var condition = ENQ_TS.ge(currentTimestamp().minus(7));
        if (sinceTimestamp != null) {
            condition = condition.and(ENQ_TS.gt(sinceTimestamp));
        }
        var inner = select(MSG_ID, ENQ_TS, USER_DATA, rn)
                .from(t)
                .where(condition)
                .asTable("x");
        return dsl.select( MSG_ID, ENQ_TS, USER_DATA)
            .from(inner)
            .where(field(name("x", "rn"), Integer.class).eq(1))
            .orderBy(ENQ_TS.desc())
            .offset(offset)
            .limit(pageSize)
            .fetch();
    }
}
