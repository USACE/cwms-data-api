package cwms.radar.data.dao;

import java.io.IOException;

import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;

public interface RatingDao
{
	void create(RatingSet ratingSet) throws IOException;
	RatingSet retrieve(String officeId, String specificationId) throws IOException;
	String retrieveRatings(String format, String names, String unit, String datum, String office, String start, String end, String timezone);
	void store(RatingSet ratingSet) throws IOException;
	void delete(String officeId, String ratingSpecId) throws IOException, RatingException;
}
