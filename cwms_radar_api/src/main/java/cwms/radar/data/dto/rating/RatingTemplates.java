package cwms.radar.data.dto.rating;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import cwms.radar.api.errors.FieldException;
import cwms.radar.data.dto.CwmsDTOPaginated;

public class RatingTemplates extends CwmsDTOPaginated{

	private List<RatingTemplate> templates;

	private RatingTemplates(){
	}

	private int offset;

	private RatingTemplates(int offset, int pageSize, Integer total, List<RatingTemplate> templates ){
		super(Integer.toString(offset), pageSize, total);
		this.templates = new ArrayList<>(templates);
		this.offset = offset;
	}

	public List<RatingTemplate> getTemplates(){
		return Collections.unmodifiableList(templates);
	}


	@Override
	public void validate() throws FieldException
	{

	}

	public static class Builder {
		private final int offset;
		private final int pageSize;
		private final Integer total;
		private List<RatingTemplate> templates;

		public Builder(int offset, int pageSize, Integer total){
			this.offset = offset;
			this.pageSize = pageSize;
			this.total = total;
		}

		public Builder templates(List<RatingTemplate> specList){
			this.templates = specList;
			return this;
		}

		public RatingTemplates build(){
			RatingTemplates retval = new RatingTemplates(offset, pageSize, total, templates);

			if(this.templates.size() == this.pageSize){
				String cursor = Integer.toString(retval.offset + retval.templates.size());
				retval.nextPage = encodeCursor(cursor,
						retval.pageSize,
						retval.total);
			} else {
				retval.nextPage = null;
			}
			return retval;
		}

	}

}

