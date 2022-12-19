package cwms.radar.data.dao;

import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import mil.army.usace.hec.cwms.rating.io.jdbc.ConnectionProvider;
import mil.army.usace.hec.cwms.rating.io.jdbc.RatingJdbcFactory;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import usace.cwms.db.dao.ifc.rating.CwmsDbRating;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_RATING_PACKAGE;

public class RatingSetDao extends JooqDao<RatingSet> implements RatingDao {


    public RatingSetDao(DSLContext dsl) {
        super(dsl);
    }

    @Override
    public void create(RatingSet ratingSet) throws IOException, RatingException {
        try {
            connection(dsl, c -> {
                // can't exist if we are creating, if it exists use store
                boolean overwriteExisting = false;
                RatingJdbcFactory.store(ratingSet, c, overwriteExisting, true);
            });
        } catch (DataAccessException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RatingException) {
                throw (RatingException) cause;
            }
            throw new IOException("Failed to create Rating", ex);
        }
    }

    @Override
    public RatingSet retrieve(RatingSet.DatabaseLoadMethod method, String officeId,
                              String specificationId, ZonedDateTime startZdt, ZonedDateTime endZdt
    ) throws IOException, RatingException {

        final RatingSet[] retval = new RatingSet[1];
        try {
            final Long start;
            if (startZdt != null) {
                start = startZdt.toInstant().toEpochMilli();
            } else {
                start = null;
            }

            final Long end;
            if (endZdt != null) {
                end = endZdt.toInstant().toEpochMilli();
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
    public void store(RatingSet ratingSet) throws IOException, RatingException {
        try {
            connection(dsl, c -> {
                boolean overwriteExisting = true;
                RatingJdbcFactory.store(ratingSet, c, overwriteExisting, true);
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
    public void delete(String officeId, String ratingSpecId) throws IOException, RatingException {
        try {
            connection(dsl, c -> delete(c, officeId, ratingSpecId));
        } catch (DataAccessException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RatingException) {
                throw (RatingException) cause;
            }
            throw new IOException("Failed to delete Rating", ex);
        }

    }

    public void delete(Connection c, String officeId, String ratingSpecId) throws SQLException {
        delete(c, DeleteRule.DELETE_ALL, ratingSpecId, officeId);
    }

    public void delete(Connection c, DeleteRule deleteRule, String ratingSpecId, String officeId)
            throws SQLException {
        CwmsDbRating cwmsDbRating = CwmsDbServiceLookup.buildCwmsDb(CwmsDbRating.class, c);
        cwmsDbRating.deleteSpecs(c, ratingSpecId, deleteRule.getRule(), officeId);
    }

    public void delete(String officeId, String specificationId, long[] effectiveDates)
            throws IOException, RatingException {

        try {
            connection(dsl, c ->
                            deleteWithRatingSet(c, officeId, specificationId, effectiveDates) //
                    // This
                    // doesn't seem to work.
            );
        } catch (DataAccessException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RatingException) {
                throw (RatingException) cause;
            }
            throw new IOException("Failed to delete Rating", ex);
        }
    }


    private void deleteWithRatingSet(Connection c, String officeId, String specificationId,
                                     long[] effectiveDates)
            throws RatingException {
        RatingSet ratingSet = RatingJdbcFactory.ratingSet(new RatingConnectionProvider(c), officeId, specificationId,
            null, null, false);
        for (final long effectiveDate : effectiveDates) {
            ratingSet.removeRating(effectiveDate);
        }

        final boolean overwriteExisting = true;
        RatingJdbcFactory.store(ratingSet, c, overwriteExisting, true);
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
