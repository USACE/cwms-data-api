/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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

package cwms.cda.data.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import mil.army.usace.hec.cwms.rating.io.jdbc.ConnectionProvider;
import mil.army.usace.hec.cwms.rating.io.jdbc.RatingJdbcFactory;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import usace.cwms.db.jooq.codegen.packages.CWMS_RATING_PACKAGE;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

public class RatingSetDao extends JooqDao<RatingSet> implements RatingDao {


    public RatingSetDao(DSLContext dsl) {
        super(dsl);
    }

    @Override
    public void create(String ratingSetXml, boolean storeTemplate) throws IOException, RatingException {
        try {
            connection(dsl, c -> {
                // can't exist if we are creating, if it exists use store
                String office = extractOfficeId(ratingSetXml);
                DSLContext context = getDslContext(c, office);
                CWMS_RATING_PACKAGE.call_STORE_RATINGS_XML__5(context.configuration(),
                        ratingSetXml, "T", storeTemplate ? "T" : "F");
            });
        } catch (DataAccessException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RatingException) {
                throw (RatingException) cause;
            }
            throw new IOException("Failed to create Rating", ex);
        }
    }

    private static String extractOfficeId(String ratingSet) throws JsonProcessingException {
        XmlMapper xmlMapper = new XmlMapper();
        JsonNode node = xmlMapper.readTree(ratingSet);
        List<JsonNode> values = node.findValues("office-id");
        String office = "";
        if (!values.isEmpty()) {
            //Getting the last instance since the order is template, spec, rating
            office = values.get(values.size() - 1).textValue();
        }
        return office;
    }

    @Override
    public RatingSet retrieve(RatingSet.DatabaseLoadMethod method, String officeId,
                              String specificationId, Instant startZdt, Instant endZdt
    ) throws IOException, RatingException {

        final RatingSet[] retval = new RatingSet[1];
        try {
            final Long start;
            if (startZdt != null) {
                start = startZdt.toEpochMilli();
            } else {
                start = null;
            }

            final Long end;
            if (endZdt != null) {
                end = endZdt.toEpochMilli();
            } else {
                end = null;
            }

            if (method == null) {
                method = RatingSet.DatabaseLoadMethod.EAGER;
            }

            RatingSet.DatabaseLoadMethod finalMethod = method;

            connection(dsl, c -> retval[0] =
                    RatingJdbcFactory.ratingSet(finalMethod, new RatingConnectionProvider(c), officeId,
                        specificationId, start, end, false));

        } catch (DataAccessException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RatingException) {
                if (cause.getMessage().contains("contains no rating templates")) {
                    return null;
                }

                throw (RatingException) cause;
            }
            throw new IOException("Failed to retrieve Rating", ex);
        }
        return retval[0];
    }

    // store/update
    @Override
    public void store(String ratingSetXml, boolean includeTemplate) throws IOException, RatingException {
        try {
            connection(dsl, c -> {
                String office = extractOfficeId(ratingSetXml);
                DSLContext context = getDslContext(c, office);
                CWMS_RATING_PACKAGE.call_STORE_RATINGS_XML__5(context.configuration(),
                        ratingSetXml, "F", includeTemplate ? "T" : "F");
            });
        } catch (DataAccessException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RatingException) {
                throw (RatingException) cause;
            }
            throw new IOException("Failed to store Rating", ex);
        }
    }

    @Override
    public void delete(String officeId, String specificationId, Instant start, Instant end) {
        Timestamp startDate = new Timestamp(start.toEpochMilli());
        Timestamp endDate = new Timestamp(end.toEpochMilli());
        dsl.connection(c->
            CWMS_RATING_PACKAGE.call_DELETE_RATINGS(
                getDslContext(c,officeId).configuration(), specificationId, startDate,
                endDate, "UTC", officeId
            )
        );
    }


    @Override
    public String retrieveRatings(String format, String names, String unit, String datum,
                                  String office, String start,
                                  String end, String timezone) {
        return CWMS_RATING_PACKAGE.call_RETRIEVE_RATINGS_F(dsl.configuration(), names, format,
                unit, datum, start, end,
                timezone, office);
    }

    private static final class RatingConnectionProvider implements ConnectionProvider {
        private final Connection c;

        private RatingConnectionProvider(Connection c) {
            this.c = c;
        }

        @Override
        public Connection getConnection() {
            return c;
        }

        @Override
        public void closeConnection(Connection connection) {
            //No-op - we will handle our connection state
        }
    }
}
