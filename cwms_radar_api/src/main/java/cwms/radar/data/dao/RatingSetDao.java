package cwms.radar.data.dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import hec.data.cwmsRating.RatingSet;
import hec.data.RatingException;
import usace.cwms.db.dao.ifc.rating.CwmsDbRating;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_RATING_PACKAGE;

public class RatingSetDao extends JooqDao<RatingSet> implements RatingDao
{
	public RatingSetDao(DSLContext dsl)
	{
		super(dsl);
	}

	@Override
	public void create(RatingSet ratingSet) throws IOException, RatingException
	{
		try
		{
			dsl.connection(c ->
			{
				// can't exist if we are creating, if it exists use store
				boolean overwriteExisting = false;
				ratingSet.storeToDatabase(c, overwriteExisting);
			});
		}
		catch(DataAccessException ex)
		{
			Throwable cause = ex.getCause();
			if(cause instanceof RatingException){
				throw (RatingException)cause;
			}
			throw new IOException("Failed to create Rating", ex);
		}
	}

	@Override
	public RatingSet retrieve(String officeId, String specificationId) throws IOException, RatingException
	{
		final RatingSet[] retval = new RatingSet[1];
		try
		{
			dsl.connection(c -> retval[0] = RatingSet.fromDatabase(RatingSet.DatabaseLoadMethod.EAGER, c, officeId, specificationId));
		}
		catch(DataAccessException ex)
		{
			Throwable cause = ex.getCause();
			if(cause instanceof RatingException){
				if(cause.getMessage().contains("contains no rating templates")){
					return null;
				}

				throw (RatingException)cause;
			}
			throw new IOException("Failed to retrieve Rating", ex);
		}
		return retval[0];
	}

	// store/update
	@Override
	public void store(RatingSet ratingSet) throws IOException, RatingException
	{
		try
		{
			dsl.connection(c ->
			{
				boolean overwriteExisting = true;
				ratingSet.storeToDatabase(c, overwriteExisting);
			});
		}
		catch(DataAccessException ex)
		{
			Throwable cause = ex.getCause();
			if(cause instanceof RatingException){
				throw (RatingException)cause;
			}
			throw new IOException("Failed to store Rating", ex);
		}
	}

	@Override
	public void delete(String officeId, String ratingSpecId) throws IOException, RatingException
	{
		try
		{
			dsl.connection(c ->
			{
//				deleteWithRatingSet(c, officeId, ratingSpecId);
				delete(c, officeId, ratingSpecId);
			});
		}
		catch(DataAccessException ex)
		{
			Throwable cause = ex.getCause();
			if(cause instanceof RatingException){
				throw (RatingException)cause;
			}
			throw new IOException("Failed to delete Rating", ex);
		}

	}

	public void delete(Connection c, String officeId, String ratingSpecId) throws SQLException, RatingException
    {
	    delete(c, DeleteRule.DELETE_ALL, ratingSpecId, officeId);
    }

	public void delete(Connection c, DeleteRule deleteRule, String ratingSpecId, String officeId)
			throws SQLException
	{
		CwmsDbRating cwmsDbRating = CwmsDbServiceLookup.buildCwmsDb(CwmsDbRating.class, c);
		cwmsDbRating.deleteSpecs(c, ratingSpecId, deleteRule.getRule(), officeId);
	}

	// This doesn't seem to work.
	private void deleteWithRatingSet(Connection c, String officeId, String ratingSpecId) throws RatingException
	{
		RatingSet ratingSet = RatingSet.fromDatabase(c, officeId, ratingSpecId);
		ratingSet.removeAllRatings();
		final boolean overwriteExisting = true;
		ratingSet.storeToDatabase(c, overwriteExisting);  // Does this actually delete?
	}

	public void delete(String officeId, String specificationId, long[] effectiveDates)
			throws IOException, RatingException
	{

		try
		{
			dsl.connection(c ->
			{
				deleteWithRatingSet(c, officeId, specificationId, effectiveDates); // This doesn't seem to work.
			});
		}
		catch(DataAccessException ex)
		{
			Throwable cause = ex.getCause();
			if(cause instanceof RatingException){
				throw (RatingException)cause;
			}
			throw new IOException("Failed to delete Rating", ex);
		}
	}


	private void deleteWithRatingSet(Connection c, String officeId, String specificationId, long[] effectiveDates)
			throws RatingException
	{
		RatingSet ratingSet = RatingSet.fromDatabase(c, officeId, specificationId);
		for(final long effectiveDate : effectiveDates)
		{
			ratingSet.removeRating(effectiveDate);
		}

		final boolean overwriteExisting = true;
		ratingSet.storeToDatabase(c, overwriteExisting);
	}


	@Override
	public String retrieveRatings(String format, String names, String unit, String datum, String office, String start, String end, String timezone)
	{
		return CWMS_RATING_PACKAGE.call_RETRIEVE_RATINGS_F(dsl.configuration(), names, format, unit, datum,
				start, end, timezone, office);
	}
}
