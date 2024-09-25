/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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
package cwms.cda.data.dao;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.measurement.Measurement;
import cwms.cda.data.dto.measurement.StreamflowMeasurement;
import cwms.cda.data.dto.measurement.SupplementalStreamflowMeasurement;
import cwms.cda.data.dto.measurement.UsgsMeasurement;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;
import mil.army.usace.hec.metadata.location.LocationTemplate;
import org.jooq.DSLContext;

import java.util.List;

import static java.util.stream.Collectors.toList;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_STREAM_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.STREAMFLOW_MEAS2_T;
import usace.cwms.db.jooq.codegen.udt.records.STREAMFLOW_MEAS2_TAB_T;

public final class MeasurementDao extends JooqDao<Measurement> {
    static final XmlMapper XML_MAPPER = buildXmlMapper();

    public MeasurementDao(DSLContext dsl) {
        super(dsl);
    }

    /**
     * Retrieve a list of measurements
     *
     * @param officeId   - the office id
     * @param locationId - the location id for filtering
     * @param unitSystem - the unit system to use for the returned data
     * @return a list of measurements
     */
    public List<Measurement> retrieveMeasurements(String officeId, String locationId, Instant minDateMask, Instant maxDateMask, String unitSystem,
                                                  Number minHeight, Number maxHeight, Number minFlow, Number maxFlow, String minNum, String maxNum,
                                                  String agencies, String qualities) {
        return connectionResult(dsl, conn -> {
            setOffice(conn, officeId);
            Timestamp minTimestamp = OracleTypeMap.buildTimestamp(minDateMask == null ? null : Date.from(minDateMask));
            Timestamp maxTimestamp = OracleTypeMap.buildTimestamp(maxDateMask == null ? null : Date.from(maxDateMask));
            TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;
            STREAMFLOW_MEAS2_TAB_T retrieved = CWMS_STREAM_PACKAGE.call_RETRIEVE_MEAS_OBJS(DSL.using(conn).configuration(), locationId, unitSystem, minTimestamp, maxTimestamp,
                    minHeight, maxHeight, minFlow, maxFlow, minNum, maxNum, agencies, qualities, timeZone.getID(), officeId);
            return retrieved.stream()
                    .map(MeasurementDao::fromJooqMeasurementRecord)
                    .collect(toList());
        });
    }

    /**
     * Store a measurement
     *
     * @param measurement  - the measurement to store
     * @param failIfExists - if true, fail if the measurement already exists
     */
    public void storeMeasurement(Measurement measurement, boolean failIfExists) {
        connection(dsl, conn -> {
            setOffice(conn, measurement.getOfficeId());
            String failIfExistsStr = formatBool(failIfExists);
            String xml = toDbXml(measurement);
            CWMS_STREAM_PACKAGE.call_STORE_MEAS_XML(DSL.using(conn).configuration(), xml, failIfExistsStr);
        });
    }

    /**
     * Delete a measurement
     *
     * @param officeId   - the office id
     * @param locationId - the location id of the measurement to delete
     */
    public void deleteMeasurements(String officeId, String locationId, Instant minDateMask, Instant maxDateMask, String unitSystem,
                                   Number minHeight, Number maxHeight, Number minFlow, Number maxFlow, String minNum, String maxNum,
                                   String agencies, String qualities) {
        connection(dsl, conn -> {
            setOffice(conn, officeId);
            Timestamp minTimestamp = minDateMask == null ? null : Timestamp.from(maxDateMask);
            Timestamp maxTimestamp = minDateMask == null ? null : Timestamp.from(maxDateMask);
            TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;
            String timeZoneId = timeZone.getID();
            CWMS_STREAM_PACKAGE.call_DELETE_STREAMFLOW_MEAS(DSL.using(conn).configuration(), locationId, unitSystem, minTimestamp, maxTimestamp,
                    minHeight, maxHeight, minFlow, maxFlow, minNum, maxNum, agencies, qualities, timeZoneId, officeId);
        });
    }

    static String toDbXml(Measurement measurement) throws JsonProcessingException {
        MeasurementXmlDto xmlDto = convertMeasurementToXmlDto(measurement);
        return XML_MAPPER.writeValueAsString(xmlDto);
    }

    static Measurement fromJooqMeasurementRecord(STREAMFLOW_MEAS2_T record) {
        LocationTemplate locationTemplate = new LocationTemplate(record.getLOCATION().getOFFICE_ID(), record.getLOCATION().getBASE_LOCATION_ID(),
                record.getLOCATION().getSUB_LOCATION_ID());
        return new Measurement.Builder()
                .withId(new CwmsId.Builder()
                        .withName(locationTemplate.getLocationId())
                        .withOfficeId(locationTemplate.getOfficeId())
                        .build())
                .withNumber(record.getMEAS_NUMBER())
                .withAgency(record.getAGENCY_ID())
                .withParty(record.getPARTY())
                .withUsed(parseBool(record.getUSED()))
                .withWmComments(record.getWM_COMMENTS())
                .withInstant(record.getDATE_TIME().toInstant())
                .withAreaUnit(record.getAREA_UNIT())
                .withFlowUnit(record.getFLOW_UNIT())
                .withHeightUnit(record.getHEIGHT_UNIT())
                .withVelocityUnit(record.getVELOCITY_UNIT())
                .withTempUnit(record.getTEMP_UNIT())
                .withStreamflowMeasurement(new StreamflowMeasurement.Builder()
                        .withFlow(record.getFLOW())
                        .withGageHeight(record.getGAGE_HEIGHT())
                        .withQuality(record.getQUALITY())
                        .build())
                .withUsgsMeasurement(new UsgsMeasurement.Builder()
                        .withAirTemp(record.getAIR_TEMP())
                        .withCurrentRating(record.getCUR_RATING_NUM())
                        .withControlCondition(record.getCTRL_COND_ID())
                        .withFlowAdjustment(record.getFLOW_ADJ_ID())
                        .withDeltaHeight(record.getDELTA_HEIGHT())
                        .withDeltaTime(record.getDELTA_TIME())
                        .withPercentDifference(record.getPCT_DIFF())
                        .withRemarks(record.getREMARKS())
                        .withShiftUsed(record.getSHIFT_USED())
                        .withWaterTemp(record.getWATER_TEMP())
                        .build())
                .withSupplementalStreamflowMeasurement(new SupplementalStreamflowMeasurement.Builder()
                        .withAvgVelocity(record.getSUPP_STREAMFLOW_MEAS().getAVG_VELOCITY())
                        .withChannelFlow(record.getSUPP_STREAMFLOW_MEAS().getCHANNEL_FLOW())
                        .withMeanGage(record.getSUPP_STREAMFLOW_MEAS().getMEAN_GAGE())
                        .withMaxVelocity(record.getSUPP_STREAMFLOW_MEAS().getMAX_VELOCITY())
                        .withOverbankFlow(record.getSUPP_STREAMFLOW_MEAS().getOVERBANK_FLOW())
                        .withOverbankArea(record.getSUPP_STREAMFLOW_MEAS().getOVERBANK_AREA())
                        .withTopWidth(record.getSUPP_STREAMFLOW_MEAS().getTOP_WIDTH())
                        .withSurfaceVelocity(record.getSUPP_STREAMFLOW_MEAS().getSURFACE_VELOCITY())
                        .withChannelMaxDepth(record.getSUPP_STREAMFLOW_MEAS().getCHANNEL_MAX_DEPTH())
                        .withMainChannelArea(record.getSUPP_STREAMFLOW_MEAS().getMAIN_CHANNEL_AREA())
                        .withOverbankMaxDepth(record.getSUPP_STREAMFLOW_MEAS().getOVERBANK_MAX_DEPTH())
                        .withEffectiveFlowArea(record.getSUPP_STREAMFLOW_MEAS().getEFFECTIVE_FLOW_AREA())
                        .withCrossSectionalArea(record.getSUPP_STREAMFLOW_MEAS().getCROSS_SECTIONAL_AREA())
                        .build())
                .build();
    }

    static MeasurementXmlDto convertMeasurementToXmlDto(Measurement meas)
    {
        return new MeasurementXmlDto.Builder()
                .withAgency(meas.getAgency())
                .withAreaUnit(meas.getAreaUnit())
                .withFlowUnit(meas.getFlowUnit())
                .withHeightUnit(meas.getHeightUnit())
                .withInstant(meas.getInstant())
                .withLocationId(meas.getLocationId())
                .withNumber(meas.getNumber())
                .withOfficeId(meas.getOfficeId())
                .withParty(meas.getParty())
                .withTempUnit(meas.getTempUnit())
                .withUsed(meas.isUsed())
                .withVelocityUnit(meas.getVelocityUnit())
                .withStreamflowMeasurement(meas.getStreamflowMeasurement())
                .withSupplementalStreamflowMeasurement(meas.getSupplementalStreamflowMeasurement())
                .withUsgsMeasurement(meas.getUsgsMeasurement())
                .withWmComments(meas.getWmComments())
                .build();
    }

    private static XmlMapper buildXmlMapper() {
        XmlMapper retVal = new XmlMapper();
        retVal.registerModule(new JavaTimeModule());
        SimpleModule module = new SimpleModule();
        module.addSerializer(Instant.class, new InstantSerializer());
        module.addDeserializer(Instant.class, new InstantDeserializer());
        retVal.registerModule(module);
        return retVal;
    }

    private static class InstantSerializer extends JsonSerializer<Instant> {
        @Override
        public void serialize(Instant value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeString(DateTimeFormatter.ISO_INSTANT.format(value));
        }
    }

    private static class InstantDeserializer extends JsonDeserializer<Instant> {
        @Override
        public Instant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            String text = p.getText();
            try {
                return Instant.parse(text);
            } catch (Exception e) {
                try {
                    // Try parsing as OffsetDateTime next
                    OffsetDateTime offsetDateTime = OffsetDateTime.parse(text, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                    return offsetDateTime.toInstant();
                } catch (Exception ex) {
                    try {
                        // Finally, fallback to parsing as ZonedDateTime
                        ZonedDateTime zonedDateTime = ZonedDateTime.parse(text, DateTimeFormatter.ISO_ZONED_DATE_TIME);
                        return zonedDateTime.toInstant();
                    } catch (Exception finalEx) {
                        // Handle or rethrow as needed
                        throw new IOException("Failed to parse date-time string: " + text, finalEx);
                    }
                }
            }
        }
    }

    @JsonRootName(value = "measurement")
    @JsonDeserialize(builder = MeasurementXmlDto.Builder.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    static final class MeasurementXmlDto {

        private final String _heightUnit;

        private final String _flowUnit;

        private final String _tempUnit;

        private final String _velocityUnit;

        private final String _areaUnit;

        private final boolean _used;
        private final String _agency;
        private final String _party;
        private final String _wmComments;
        private final StreamflowMeasurement _streamflowMeasurement;
        private final SupplementalStreamflowMeasurement _supplementalStreamflowMeasurement;
        private final UsgsMeasurement _usgsMeasurement;
        private final String _locationId;
        private final String _officeId;
        private final String _number;
        private final Instant _instant;

        private MeasurementXmlDto(Builder builder) {
            this._heightUnit = builder._heightUnit;
            this._flowUnit = builder._flowUnit;
            this._tempUnit = builder._tempUnit;
            this._velocityUnit = builder._velocityUnit;
            this._areaUnit = builder._areaUnit;
            this._used = builder._used;
            this._agency = builder._agency;
            this._party = builder._party;
            this._wmComments = builder._wmComments;
            this._locationId = builder._locationId;
            this._officeId = builder._officeId;
            this._number = builder._number;
            this._instant = builder._instant;
            this._streamflowMeasurement = builder._streamflowMeasurement;
            this._supplementalStreamflowMeasurement = builder._supplementalStreamflowMeasurement;
            this._usgsMeasurement = builder._usgsMeasurement;
        }

        @JacksonXmlProperty(isAttribute = true)
        public String getHeightUnit() {
            return _heightUnit;
        }

        @JacksonXmlProperty(isAttribute = true)
        public String getFlowUnit() {
            return _flowUnit;
        }

        @JacksonXmlProperty(isAttribute = true)
        public String getTempUnit() {
            return _tempUnit;
        }

        @JacksonXmlProperty(isAttribute = true)
        public String getVelocityUnit() {
            return _velocityUnit;
        }

        @JacksonXmlProperty(isAttribute = true)
        public String getAreaUnit() {
            return _areaUnit;
        }

        @JacksonXmlProperty(isAttribute = true)
        public boolean isUsed() {
            return _used;
        }

        @JacksonXmlProperty(isAttribute = true)
        public String getOfficeId() {
            return _officeId;
        }

        public String getAgency() {
            return _agency;
        }

        public String getParty() {
            return _party;
        }

        public String getWmComments() {
            return _wmComments;
        }

        @JsonProperty("location")
        public String getLocationId() {
            return _locationId;
        }

        @JsonProperty("date")
        public Instant getInstant() {
            return _instant;
        }

        public String getNumber() {
            return _number;
        }

        @JsonProperty("stream-flow-measurement")
        public StreamflowMeasurement getStreamflowMeasurement() {
            return _streamflowMeasurement;
        }

        @JsonProperty("supplemental-stream-flow-measurement")
        public SupplementalStreamflowMeasurement getSupplementalStreamflowMeasurement() {
            return _supplementalStreamflowMeasurement;
        }

        public UsgsMeasurement getUsgsMeasurement() {
            return _usgsMeasurement;
        }

        @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
        static final class Builder {
            private String _heightUnit;
            private String _flowUnit;
            private String _tempUnit;
            private String _velocityUnit;
            private String _areaUnit;
            private boolean _used;
            private String _agency;
            private String _party;
            private String _wmComments;
            private StreamflowMeasurement _streamflowMeasurement;
            private SupplementalStreamflowMeasurement _supplementalStreamflowMeasurement;
            private UsgsMeasurement _usgsMeasurement;
            private String _officeId;
            private String _locationId;
            private Instant _instant;
            private String _number;

            private Builder withHeightUnit(String heightUnit) {
                this._heightUnit = heightUnit;
                return this;
            }

            private Builder withFlowUnit(String flowUnit) {
                this._flowUnit = flowUnit;
                return this;
            }

            private Builder withTempUnit(String tempUnit) {
                this._tempUnit = tempUnit;
                return this;
            }

            private Builder withVelocityUnit(String velocityUnit) {
                this._velocityUnit = velocityUnit;
                return this;
            }

            private Builder withAreaUnit(String areaUnit) {
                this._areaUnit = areaUnit;
                return this;
            }

            private Builder withUsed(boolean used) {
                this._used = used;
                return this;
            }

            private Builder withAgency(String agency) {
                this._agency = agency;
                return this;
            }

            private Builder withParty(String party) {
                this._party = party;
                return this;
            }

            private Builder withWmComments(String wmComments) {
                this._wmComments = wmComments;
                return this;
            }

            @JsonProperty("stream-flow-measurement")
            private Builder withStreamflowMeasurement(StreamflowMeasurement streamflowMeasurement) {
                this._streamflowMeasurement = streamflowMeasurement;
                return this;
            }

            @JsonProperty("supplemental-stream-flow-measurement")
            private Builder withSupplementalStreamflowMeasurement(SupplementalStreamflowMeasurement supplementalStreamflowMeasurement) {
                this._supplementalStreamflowMeasurement = supplementalStreamflowMeasurement;
                return this;
            }

            private Builder withUsgsMeasurement(UsgsMeasurement usgsMeasurement) {
                this._usgsMeasurement = usgsMeasurement;
                return this;
            }

            private Builder withOfficeId(String officeId) {
                _officeId = officeId;
                return this;
            }

            @JsonProperty("location")
            private Builder withLocationId(String locationId) {
                _locationId = locationId;
                return this;
            }

            private Builder withNumber(String number) {
                 _number = number;
                return this;
            }

            @JsonProperty("date")
            private Builder withInstant(Instant instant) {
                _instant = instant;
                return this;
            }

            private MeasurementXmlDto build() {
                return new MeasurementXmlDto(this);
            }
        }
    }
}
