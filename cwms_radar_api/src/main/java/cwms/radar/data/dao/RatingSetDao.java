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
	private static final Logger logger = Logger.getLogger(RatingSetDao.class.getName());

	public RatingSetDao(DSLContext dsl)
	{
		super(dsl);
	}

	@Override
	public void create(RatingSet ratingSet) throws IOException
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
			throw new IOException("Failed to create Rating");
		}
	}

	@Override
	public RatingSet retrieve(String officeId, String specificationId) throws IOException
	{
		final RatingSet[] retval = new RatingSet[1];
		try
		{
			dsl.connection(c -> retval[0] = RatingSet.fromDatabase(c, officeId, specificationId));
		}
		catch(DataAccessException ex)
		{
			throw new IOException("Failed to retrieve Rating");
		}
		return retval[0];
	}

	// store/update
	@Override
	public void store(RatingSet ratingSet) throws IOException
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
			throw new IOException("Failed to store Rating");
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
			throw new IOException("Failed to delete Rating");
		}

	}

	public void delete(String officeId, String specificationId, long[] effectiveDates) throws IOException
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
			throw new IOException("Failed to delete Rating");
		}

	}


	@Override
	public String retrieveRatings(String format, String names, String unit, String datum, String office, String start, String end, String timezone)
	{
		return CWMS_RATING_PACKAGE.call_RETRIEVE_RATINGS_F(dsl.configuration(), names, format, unit, datum,
				start, end, timezone, office);
	}
}
