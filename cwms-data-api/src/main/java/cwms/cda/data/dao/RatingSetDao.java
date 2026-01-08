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
import cwms.cda.data.dto.VerticalDatumInfo;
import cwms.cda.data.dto.rating.RatingSpec;
import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import mil.army.usace.hec.cwms.rating.io.jdbc.ConnectionProvider;
import mil.army.usace.hec.cwms.rating.io.jdbc.RatingJdbcFactory;
import org.jetbrains.annotations.Nullable;
import org.jooq.ConnectionRunnable;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_RATING_PACKAGE;


public class RatingSetDao extends JooqDao<RatingSet> implements RatingDao {

    public RatingSetDao(DSLContext dsl) {
        super(dsl);
    }

    @Override
    public void create(String ratingSetXml, boolean replaceBaseCurve, VerticalDatum vd) throws IOException, RatingException {
        connection(dsl, connection -> storeWithDefaultDatum(ratingSetXml, replaceBaseCurve, true, vd, connection));
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

    private static String extractLocationId(String ratingSet) throws JsonProcessingException {
        XmlMapper xmlMapper = new XmlMapper();
        JsonNode node = xmlMapper.readTree(ratingSet);
        List<JsonNode> values = node.findValues("location-id");
        String location = "";
        if (!values.isEmpty()) {
            //Getting the last instance since the order is template, spec, rating
            location = values.get(values.size() - 1).textValue();
        }
        return location;
    }

    @Override
    public String retrieveLatestXML(String officeId, String specificationId) {
        return connectionResult(dsl, c -> {
            DSLContext context = getDslContext(c, officeId);
            return CWMS_RATING_PACKAGE.call_RETRIEVE_EFF_RATINGS_XML_F(context.configuration(), specificationId,
                    Timestamp.from(Instant.now()), Timestamp.from(Instant.now()), null, officeId);
        });
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

            connection(dsl, c -> {
                setOffice(c, officeId);
                retval[0] = RatingJdbcFactory.ratingSet(finalMethod, new RatingConnectionProvider(c), officeId,
                                specificationId, start, end, false);
            });


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
    public void store(String ratingSetXml, boolean replaceBaseCurve, VerticalDatum vd) throws IOException, RatingException {
        connection(dsl, connection -> storeWithDefaultDatum(ratingSetXml, replaceBaseCurve, false, vd, connection));
    }

    private static void storeRatingSetXml(String ratingSetXml, boolean replaceBaseCurve, boolean failIfExists, Connection c) throws RatingException, IOException {
        try {
            String office = extractOfficeId(ratingSetXml);
            DSLContext context = getDslContext(c, office);
            String errs = CWMS_RATING_PACKAGE.call_STORE_RATINGS_XML__5(context.configuration(),
                    ratingSetXml, formatBool(failIfExists), formatBool(replaceBaseCurve));
            if (errs != null && !errs.isEmpty())
            {
                throw new DataAccessException("Failed to store Rating", new RatingException(errs));
            }
        } catch (DataAccessException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RatingException) {
                throw (RatingException) cause;
            }
            throw new IOException("Failed to store Rating", ex);
        }
    }

    private void storeWithDefaultDatum(String ratingSetXml, boolean replaceBaseCurve, boolean failIfExists,
                                       VerticalDatum vd, Connection connection) throws Throwable {
        String office = extractOfficeId(ratingSetXml);
        String locationId = extractLocationId(ratingSetXml);
        DSLContext dslContext = getDslContext(connection, office);
        withLocalAndDefaultDatum(locationId, office, vd, dslContext, c -> storeRatingSetXml(ratingSetXml, replaceBaseCurve, failIfExists, c));
    }

    protected void withLocalAndDefaultDatum(String locationId, String officeId, @Nullable VerticalDatum targetDatum, DSLContext dslContext, ConnectionRunnable cr) {
        boolean localDatumAdded = false;
        try {
            //if converting to NAVD88 or NGVD29, we need to set the local datum to the native datum temporarily or the conversion will fail in the db
            if(targetDatum == VerticalDatum.NAVD88 || targetDatum == VerticalDatum.NGVD29) {
                String vertDatum = CWMS_LOC_PACKAGE.call_GET_VERTICAL_DATUM_INFO_F__2(dslContext.configuration(), locationId, "m", officeId);
                if(vertDatum != null)
                {
                    XmlMapper xmlMapper = new XmlMapper();
                    VerticalDatumInfo vdi = xmlMapper.readValue(vertDatum, VerticalDatumInfo.class);
                    String nativeDatum = vdi.getNativeDatum();
                    // Only set local datum temporarily if native datum is NAVD88 or NGVD29 to allow conversion
                    // If native datum is unknown for some reason then just set to the target datum since there is no conversion needed anyways
                    if(VerticalDatum.NAVD88.toString().equals(nativeDatum) || VerticalDatum.NGVD29.toString().equals(nativeDatum)) {
                        CWMS_LOC_PACKAGE.call_SET_LOCAL_VERT_DATUM_NAME__2(dslContext.configuration(), locationId, nativeDatum, "T", officeId);
                        localDatumAdded = true;
                    } else if(nativeDatum == null || "UNKNOWN".equalsIgnoreCase(nativeDatum)) {
                        CWMS_LOC_PACKAGE.call_SET_LOCAL_VERT_DATUM_NAME__2(dslContext.configuration(), locationId, targetDatum.toString(), "T", officeId);
                        localDatumAdded = true;
                    }
                }
            }
            withDefaultDatum(targetDatum, dslContext, cr);
        } catch (IOException e) {
            throw new DataAccessException("Failed to parse vertical datum info for location " + locationId, e);
        } finally {
            if(localDatumAdded) {
                CWMS_LOC_PACKAGE.call_DELETE_LOCAL_VERT_DATUM_NAME__2(dslContext.configuration(), locationId, officeId);
            }
        }
    }

    @Override
    public void delete(String officeId, String specificationId, Instant start, Instant end) {
        Timestamp startDate = new Timestamp(start.toEpochMilli());
        Timestamp endDate = new Timestamp(end.toEpochMilli());
        dsl.connection(c ->
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
        private final Connection conn;

        private RatingConnectionProvider(Connection conn) {
            this.conn = conn;
        }

        @Override
        public Connection getConnection() {
            return conn;
        }

        @Override
        public void closeConnection(Connection connection) {
            //No-op - we will handle our connection state
        }
    }
}
