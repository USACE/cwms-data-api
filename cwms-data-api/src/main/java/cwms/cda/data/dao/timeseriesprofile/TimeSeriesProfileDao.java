package cwms.cda.data.dao.timeseriesprofile;

import java.util.List;

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PROFILE_PACKAGE;

public class TimeSeriesProfileDao extends JooqDao<TimeSeriesProfile>
{
	public TimeSeriesProfileDao(DSLContext dsl)
	{
		super(dsl);
	}
	public void storeTimeSeriesProfile(TimeSeriesProfile timeSeriesProfile, boolean failIfExists) {

		connection(dsl, conn -> {
			List<String> parameterList = timeSeriesProfile.getParameterList();
			String parameterString = parameterList.get(0);
			for(int i=1; i<parameterList.size(); i++)
			{
				parameterString += "," + parameterList.get(i);
			}
			CWMS_TS_PROFILE_PACKAGE.call_STORE_TS_PROFILE(DSL.using(conn).configuration(),timeSeriesProfile.getLocationId(),
					timeSeriesProfile.getKeyParameter(),
					parameterString, null, null, "F","T",timeSeriesProfile.getOfficeId());
		});
	}
	public void retrieveTimeSeriesProfile()
	{
		// TODO
	}
	public void deleteTimeSeriesProfile()
	{
		// TODO
	}
	public void copyTimeSeriesProfile()
	{
		// TODO
	}
	public void retrieveTimeSeriesProfiles()
	{
		// TODO
	}
}
