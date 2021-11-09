package cwms.radar.data.dao;

import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;

public enum DeleteRule
{
	// Not obvious which of the constants in CWMS_UTIL_PACKAGE are Delete rule and which aren't.
	DELETE_INSERT(CWMS_UTIL_PACKAGE.DELETE_INSERT.toString()),
	DELETE_ALL(CWMS_UTIL_PACKAGE.DELETE_ALL.toString()),
	DELETE_DATA(CWMS_UTIL_PACKAGE.DELETE_DATA.toString()),
	DELETE_KEY(CWMS_UTIL_PACKAGE.DELETE_KEY.toString()),
	DELETE_LOC(CWMS_UTIL_PACKAGE.DELETE_LOC.toString()),
	DELETE_LOC_CASCADE(CWMS_UTIL_PACKAGE.DELETE_LOC_CASCADE.toString()),
	DELETE_TS_ID(CWMS_UTIL_PACKAGE.DELETE_TS_ID.toString()),
	DELETE_TS_DATA(CWMS_UTIL_PACKAGE.DELETE_TS_DATA.toString()),
	DELETE_TS_CASCADE(CWMS_UTIL_PACKAGE.DELETE_TS_CASCADE.toString())
	;

	private final String rule;

	DeleteRule(String rule)
	{
		this.rule = rule;
	}

	public String getRule()
	{
		return rule;
	}
}
