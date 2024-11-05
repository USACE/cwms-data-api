/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "A representation of a location category")
@JsonRootName("location_category")
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
public class LocationCategory extends CwmsDTO
{
	private final String id;
	private final String description;

	public LocationCategory(@JsonProperty("office-id") String catDbOfficeId, @JsonProperty("id") String locCategoryId, @JsonProperty("description") String locCategoryDesc)
	{
		super(catDbOfficeId);
		this.id = locCategoryId;
		this.description = locCategoryDesc;
	}

	public String getId()
	{
		return id;
	}

	public String getDescription()
	{
		return description;
	}

	@Override
	public boolean equals(Object o)
	{
		if(this == o)
		{
			return true;
		}
		if(o == null || getClass() != o.getClass())
		{
			return false;
		}

		final LocationCategory that = (LocationCategory) o;

		if(getOfficeId() != null ? !getOfficeId().equals(that.getOfficeId()) : that.getOfficeId() != null)
		{
			return false;
		}
		if(getId() != null ? !getId().equals(that.getId()) : that.getId() != null)
		{
			return false;
		}
		return getDescription() != null ? getDescription().equals(
				that.getDescription()) : that.getDescription() == null;
	}

	@Override
	public int hashCode()
	{
		int result = getOfficeId() != null ? getOfficeId().hashCode() : 0;
		result = 31 * result + (getId() != null ? getId().hashCode() : 0);
		return result;
	}
}
