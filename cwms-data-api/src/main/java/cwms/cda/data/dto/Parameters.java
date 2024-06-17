/*
 * Copyright (c) 2024. Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved. HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval from HEC
 */

package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;

public class Parameters extends CwmsDTO
{

	public Parameters(String office)
	{
		super(office);
	}

	@Override
	public void validate() throws FieldException
	{

	}
}
