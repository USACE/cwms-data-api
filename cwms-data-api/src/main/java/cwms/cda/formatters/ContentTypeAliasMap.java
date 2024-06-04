/*
 * Copyright (c) 2024. Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved. HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval from HEC
 */

package cwms.cda.formatters;

import java.util.HashMap;
import java.util.Map;

public final class ContentTypeAliasMap
{
	private final Map<String, ContentType> _contentTypeMap = new HashMap<>();

	public ContentType getContentType(String alias)
	{
		return _contentTypeMap.get(alias);
	}

	public void addContentType(String alias, ContentType contentType)
	{
		_contentTypeMap.put(alias, contentType);
	}
}
