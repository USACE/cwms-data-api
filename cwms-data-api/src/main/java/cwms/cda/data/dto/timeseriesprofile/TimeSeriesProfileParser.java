package cwms.cda.data.dto.timeseriesprofile;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;

@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
@JsonDeserialize(builder = TimeSeriesProfileParser.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TimeSeriesProfileParser extends CwmsDTO
{
	private final String locationId;
	private final String keyParameter;
	private final char recordDelimiter;
	private final char fieldDelimiter;
	private final  String timeFormat;
	private final String timeZone;
	private final int timeField;
	private final List<ParameterInfo> parameterInfo;
	private final boolean timeInTwoFields;
	protected TimeSeriesProfileParser(Builder builder)
	{
		super(builder.officeId);
		locationId = builder.locationId;
		keyParameter = builder.keyParameter;
		recordDelimiter = builder.recordDelimiter;
		fieldDelimiter = builder.fieldDelimiter;
		timeFormat = builder.timeFormat;
		timeZone = builder.timeZone;
		timeField = builder.timeField;
		parameterInfo = builder.parameterInfo;
		timeInTwoFields = builder.timeInTwoFields;
	}

	@Override
	public void validate() throws FieldException
	{

	}


	public String getLocationId()
	{
		return locationId;
	}

	public String getKeyParameter()
	{
		return keyParameter;
	}
	public String getRecordDelimiter(){ return String.valueOf(recordDelimiter); }
	public String getFieldDelimiter(){ return String.valueOf(fieldDelimiter); }
	public List<ParameterInfo> getParameterInfo ()
	{
		return parameterInfo;
	}

	public String getTimeFormat()
	{
		return timeFormat;
	}

	public String getTimeZone()
	{
		return timeZone;
	}

	public BigInteger getTimeField()
	{
		return BigInteger.valueOf(timeField);
	}

	public String getTimeInTwoFields()
	{
		return timeInTwoFields?"T":"F";
	}

	@Override
	public int hashCode() {
		int result = Objects.hashCode(getLocationId());
		result = 31 * result + Objects.hashCode(getFieldDelimiter());
		result = 31 * result + Objects.hashCode(getOfficeId());
		result = 31 * result + Objects.hashCode(getKeyParameter());
		result = 31 * result + Objects.hashCode(getTimeField());
		result = 31 * result + Objects.hashCode(getTimeFormat());
		result = 31 * result + Objects.hashCode(getParameterInfo());
		result = 31 * result + Objects.hashCode(getRecordDelimiter());
		result = 31 * result + Objects.hashCode(getTimeZone());
		result = 31 * result + Objects.hashCode(getTimeInTwoFields());
		return result;
	}
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TimeSeriesProfileParser that = (TimeSeriesProfileParser) o;
		return Objects.equals(getLocationId(), that.getLocationId())
				&& Objects.equals(getFieldDelimiter(), that.getFieldDelimiter())
				&& Objects.equals(getOfficeId(), that.getOfficeId())
				&& Objects.equals(getKeyParameter(), that.getKeyParameter())
				&& Objects.equals(getTimeField(), that.getTimeField())
				&& Objects.equals(getTimeFormat(), that.getTimeFormat())
				&& Objects.equals(getParameterInfo(), that.getParameterInfo())
				&& Objects.equals(getRecordDelimiter(), that.getRecordDelimiter())
				&& Objects.equals(getTimeZone(), that.getTimeZone())
				&& Objects.equals(getTimeInTwoFields(), that.getTimeInTwoFields());
	}

	@JsonPOJOBuilder
	@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
	public static final class Builder {
		private String officeId;
		private List<ParameterInfo> parameterInfo;
		private String keyParameter;
		private char recordDelimiter;
		private char fieldDelimiter;
		private String timeFormat;
		private String timeZone;
		private int timeField;
		private boolean timeInTwoFields;
		private String locationId;

		public TimeSeriesProfileParser.Builder withLocationId(String locationId) {
			this.locationId = locationId;
			return this;
		}

		public TimeSeriesProfileParser.Builder withKeyParameter(String keyParameter) {
			this.keyParameter = keyParameter;
			return this;
		}
		public TimeSeriesProfileParser.Builder withRecordDelimiter(char delimiter) {
			this.recordDelimiter = delimiter;
			return this;
		}
		public TimeSeriesProfileParser.Builder withFieldDelimiter(char delimiter) {
			this.fieldDelimiter = delimiter;
			return this;
		}
		public TimeSeriesProfileParser.Builder withTimeFormat(String timeFormat)
		{
			this.timeFormat = timeFormat;
			return this;
		}
		public TimeSeriesProfileParser.Builder withTimeZone(String timeZone)
		{
			this.timeZone = timeZone;
			return this;
		}
		public TimeSeriesProfileParser.Builder withTimeField(int field)
		{
			this.timeField = field;
			return this;
		}
		public TimeSeriesProfileParser.Builder withTimeInTwoFields(boolean timeInTwoFields)
		{
			this.timeInTwoFields = timeInTwoFields;
			return this;
		}
		public TimeSeriesProfileParser.Builder withParameterInfoList(List<ParameterInfo> parameterInfoList)
		{
			this.parameterInfo =parameterInfoList;
				return this;
		}

		public TimeSeriesProfileParser.Builder withOfficeId(String officeId) {
			this.officeId = officeId;
			return this;
		}


		public TimeSeriesProfileParser build() {
			return new TimeSeriesProfileParser(this);
		}
	}

}
