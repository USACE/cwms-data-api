/*
 * Copyright (c) 2024. Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved. HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval from HEC
 */

package cwms.cda.formatters;

import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.annotations.FormattableWith;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

public final class ContentTypeAliasMap
{
	private final Map<String, ContentType> _contentTypeMap = new HashMap<>();
	private static final Map<Class<? extends CwmsDTOBase>, ContentTypeAliasMap> ALIAS_MAP = new HashMap<>();

	private ContentTypeAliasMap()
	{
	}

	private ContentTypeAliasMap(Class<? extends CwmsDTOBase> dtoClass)
	{
		FormattableWith[] formats = dtoClass.getAnnotationsByType(FormattableWith.class);
		for (FormattableWith format : formats)
		{
			ContentType type = new ContentType(format.contentType());

			for (String alias : format.aliases())
			{
				_contentTypeMap.put(alias, type);
			}
		}
	}

	public static ContentTypeAliasMap forDtoClass(@NotNull Class<? extends CwmsDTOBase> dtoClass)
	{
		return ALIAS_MAP.computeIfAbsent(dtoClass, ContentTypeAliasMap::new);
	}

	public static ContentTypeAliasMap empty()
	{
		return new ContentTypeAliasMap();
	}

	public ContentType getContentType(String alias)
	{
		return _contentTypeMap.get(alias);
	}
}
