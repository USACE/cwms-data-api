package cwms.radar.data.dao;

import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;

public enum StoreRule
{
	// Not obvious which of the constants in CWMS_UTIL_PACKAGE are store rule and which aren't.
	REPLACE_ALL(CWMS_UTIL_PACKAGE.REPLACE_ALL.toString()),
	DO_NOT_REPLACE(CWMS_UTIL_PACKAGE.DO_NOT_REPLACE.toString()),
	REPLACE_MISSING_VALUES_ONLY(CWMS_UTIL_PACKAGE.REPLACE_MISSING_VALUES_ONLY.toString()),
	REPLACE_WITH_NON_MISSING(CWMS_UTIL_PACKAGE.REPLACE_WITH_NON_MISSING.toString()),
	DELETE_INSERT(CWMS_UTIL_PACKAGE.DELETE_INSERT.toString());

	private final String rule;

	StoreRule(String rule)
	{
		this.rule = rule;
	}

	public String getRule()
	{
		return rule;
	}
}
