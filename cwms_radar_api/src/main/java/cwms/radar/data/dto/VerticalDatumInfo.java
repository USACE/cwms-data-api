package cwms.radar.data.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

@XmlRootElement(name="vertical-datum-info")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonDeserialize(builder = VerticalDatumInfo.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class VerticalDatumInfo
{
	@XmlAttribute
	String office;

	@XmlAttribute
	String unit;
	String location;

	@XmlElement(name="native-datum")
	String nativeDatum;
	Double elevation;

	@XmlElement(name="offset")
	VerticalDatumInfo.Offset[] offsets;

	private VerticalDatumInfo()
	{
	}

	public String getOffice()
	{
		return office;
	}

	public String getUnit()
	{
		return unit;
	}

	public String getLocation()
	{
		return location;
	}

	public String getNativeDatum()
	{
		return nativeDatum;
	}

	public Double getElevation()
	{
		return elevation;
	}

	public Offset[] getOffsets()
	{
		return offsets;
	}

	@XmlAccessorType(XmlAccessType.FIELD)
	public static class Offset {
		@XmlAttribute
		boolean estimate;

		@XmlElement(name="to-datum")
		String toDatum;

		Double value;

		public boolean isEstimate()
		{
			return estimate;
		}

		public String getToDatum()
		{
			return toDatum;
		}

		public Double getValue()
		{
			return value;
		}

		private Offset()
		{
		}

		public Offset(boolean isEstimate, String toDatum, Double value)
		{
			this.estimate = isEstimate;
            this.toDatum = toDatum;
            this.value = value;
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

			final Offset offset = (Offset) o;

			if(isEstimate() != offset.isEstimate())
			{
				return false;
			}
			if(getToDatum() != null ? !getToDatum().equals(offset.getToDatum()) : offset.getToDatum() != null)
			{
				return false;
			}
			return getValue() != null ? getValue().equals(offset.getValue()) : offset.getValue() == null;
		}

		@Override
		public int hashCode()
		{
			int result = (isEstimate() ? 1 : 0);
			result = 31 * result + (getToDatum() != null ? getToDatum().hashCode() : 0);
			result = 31 * result + (getValue() != null ? getValue().hashCode() : 0);
			return result;
		}
	}

	private VerticalDatumInfo(VerticalDatumInfo.Builder builder){
		this.office = builder.office;
        this.unit = builder.unit;
        this.location = builder.location;
        this.nativeDatum = builder.nativeDatum;
        this.elevation = builder.elevation;
        this.offsets = builder.offsets;
	}

	@JsonPOJOBuilder
	@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
	public static class Builder {
		String office;
		String unit;
		String location;
		String nativeDatum;
		Double elevation;
		Offset[] offsets;


		public VerticalDatumInfo.Builder withOffice(String office)
		{
			this.office = office;
			return this;
		}

		public VerticalDatumInfo.Builder withUnit(String unit)
        {
            this.unit = unit;
            return this;
        }

		public VerticalDatumInfo.Builder withLocation(String location)
        {
            this.location = location;
            return this;
        }

		public VerticalDatumInfo.Builder withNativeDatum(String nativeDatum)
        {
            this.nativeDatum = nativeDatum;
            return this;
        }

		public VerticalDatumInfo.Builder withElevation(Double elevation)
        {
            this.elevation = elevation;
            return this;
        }

		public VerticalDatumInfo.Builder withOffsets(VerticalDatumInfo.Offset[] offsets)
        {
            this.offsets = offsets;
            return this;
        }

		public VerticalDatumInfo.Builder withOffset(boolean isEstimate, String toDatum, Double value)
		{
			this.offsets = new Offset[]{new Offset(isEstimate, toDatum, value)};
			return this;
		}

		public VerticalDatumInfo build () {
			return new VerticalDatumInfo(this);
		}
	}
}
