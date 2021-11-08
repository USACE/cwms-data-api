package cwms.radar.data.dao;

import java.io.IOException;
import java.util.logging.Logger;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
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
				RatingSet ratingSet = RatingSet.fromDatabase(c, officeId, ratingSpecId);
				ratingSet.removeAllRatings();
				final boolean overwriteExisting = true;
				ratingSet.storeToDatabase(c, overwriteExisting);  // Does this actually delete?
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

	public void delete(String officeId, String specificationId, long[] effectiveDates)
			throws IOException, RatingException
	{

		try
		{
			dsl.connection(c ->
			{
				RatingSet ratingSet = RatingSet.fromDatabase(c, officeId, specificationId);
				for(final long effectiveDate : effectiveDates)
				{
					ratingSet.removeRating(effectiveDate);

				}
				final boolean overwriteExisting = true;
				ratingSet.storeToDatabase(c, overwriteExisting);
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


	@Override
	public String retrieveRatings(String format, String names, String unit, String datum, String office, String start, String end, String timezone)
	{
		return CWMS_RATING_PACKAGE.call_RETRIEVE_RATINGS_F(dsl.configuration(), names, format, unit, datum,
				start, end, timezone, office);
	}
}
