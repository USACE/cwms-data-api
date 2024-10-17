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

import com.fasterxml.jackson.annotation.JsonCreator;
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
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.measurement.Measurement;
import cwms.cda.data.dto.measurement.StreamflowMeasurement;
import cwms.cda.data.dto.measurement.SupplementalStreamflowMeasurement;
import cwms.cda.data.dto.measurement.UsgsMeasurement;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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
            return retrieveMeasurementsJooq(conn, officeId, locationId, unitSystem, minHeight, maxHeight, minFlow, maxFlow, minNum, maxNum, agencies, qualities, minTimestamp, maxTimestamp, timeZone);
        });
    }

    private static List<Measurement> retrieveMeasurementsJooq(Connection conn, String officeId, String locationId, String unitSystem, Number minHeight, Number maxHeight, Number minFlow, Number maxFlow, String minNum, String maxNum, String agencies, String qualities, Timestamp minTimestamp, Timestamp maxTimestamp, TimeZone timeZone) {
        STREAMFLOW_MEAS2_TAB_T retrieved = CWMS_STREAM_PACKAGE.call_RETRIEVE_MEAS_OBJS(DSL.using(conn).configuration(), locationId, unitSystem, minTimestamp, maxTimestamp,
                minHeight, maxHeight, minFlow, maxFlow, minNum, maxNum, agencies, qualities, timeZone.getID(), officeId);
        List<Measurement> retVal = retrieved.stream()
                .map(MeasurementDao::fromJooqMeasurementRecord)
                .collect(toList());
        if(retVal.isEmpty()) {
            throw new NotFoundException("No measurements found.");
        }
        return retVal;
    }

    /**
     * Store a list of measurements
     * @param measurements - the measurements to store
     * @param failIfExists - if true, fail if a measurement already exists
     */
    public void storeMeasurements(List<Measurement> measurements, boolean failIfExists) {
        connection(dsl, conn -> storeMeasurementsJooq(conn, measurements, failIfExists));
    }

    private void storeMeasurementsJooq(Connection conn, List<Measurement> measurements, boolean failIfExists) throws SQLException, JsonProcessingException {
        if(!measurements.isEmpty()) {
            Measurement measurement = measurements.get(0);
            setOffice(conn, measurement.getOfficeId());
            String failIfExistsStr = formatBool(failIfExists);
            String xml = toDbXml(measurements);
            CWMS_STREAM_PACKAGE.call_STORE_MEAS_XML(DSL.using(conn).configuration(), xml, failIfExistsStr);
        }
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
            Timestamp minTimestamp = OracleTypeMap.buildTimestamp(minDateMask == null ? null : Date.from(minDateMask));
            Timestamp maxTimestamp = OracleTypeMap.buildTimestamp(maxDateMask == null ? null : Date.from(maxDateMask));
            TimeZone timeZone = OracleTypeMap.GMT_TIME_ZONE;
            String timeZoneId = timeZone.getID();
            verifyMeasurementsExists(conn, officeId, locationId, minNum, maxNum);
            CWMS_STREAM_PACKAGE.call_DELETE_STREAMFLOW_MEAS(DSL.using(conn).configuration(), locationId, unitSystem, minTimestamp, maxTimestamp,
                    minHeight, maxHeight, minFlow, maxFlow, minNum, maxNum, agencies, qualities, timeZoneId, officeId);
        });
    }

    private void verifyMeasurementsExists(Connection conn, String officeId, String locationId, String minNum, String maxNum) {
        List<Measurement> measurements = retrieveMeasurementsJooq(conn, officeId, locationId, UnitSystem.EN.toString(),
                null, null, null, null, minNum, maxNum, null, null, null, null, OracleTypeMap.GMT_TIME_ZONE);
        if (measurements.isEmpty()) {
            throw new NotFoundException("Could not find measurements for " + locationId + " in office " + officeId + ".");
        }
    }

    static String toDbXml(List<Measurement> measurements) throws JsonProcessingException {
        MeasurementsXmlDto xmlDto = convertMeasurementsToXmlDto(measurements);
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

    static MeasurementsXmlDto convertMeasurementsToXmlDto(List<Measurement> measurements) {
        return new MeasurementsXmlDto.Builder()
                .withMeasurements(measurements.stream()
                        .map(MeasurementDao::convertMeasurementToXmlDto)
                        .collect(toList()))
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

    @JacksonXmlRootElement(localName = "measurements")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonDeserialize(builder = MeasurementsXmlDto.Builder.class)
    static class MeasurementsXmlDto {

        @JacksonXmlElementWrapper(useWrapping = false)
        @JacksonXmlProperty(localName = "measurement")
        private final List<MeasurementXmlDto> measurements;

        private MeasurementsXmlDto(Builder builder) {
            this.measurements = builder.measurements;
        }

        public List<MeasurementXmlDto> getMeasurements() {
            return measurements;
        }
        static class Builder {
            @JacksonXmlElementWrapper(useWrapping = false) // Disable wrapping for the collection
            @JacksonXmlProperty(localName = "measurement")
            private List<MeasurementXmlDto> measurements = new ArrayList<>();

            Builder withMeasurements(List<MeasurementXmlDto> measurements) {
                this.measurements = measurements;
                return this;
            }

            @JsonCreator
            MeasurementsXmlDto build() {
                return new MeasurementsXmlDto(this);
            }
        }
    }


    @JsonRootName(value = "measurement")
    @JsonDeserialize(builder = MeasurementXmlDto.Builder.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    static final class MeasurementXmlDto {

        private final String heightUnit;

        private final String flowUnit;

        private final String tempUnit;

        private final String velocityUnit;

        private final String areaUnit;

        private final boolean used;
        private final String agency;
        private final String party;
        private final String wmComments;
        private final StreamflowMeasurement streamflowMeasurement;
        private final SupplementalStreamflowMeasurement supplementalStreamflowMeasurement;
        private final UsgsMeasurement usgsMeasurement;
        private final String locationId;
        private final String officeId;
        private final String number;
        private final Instant instant;

        private MeasurementXmlDto(Builder builder) {
            this.heightUnit = builder.heightUnit;
            this.flowUnit = builder.flowUnit;
            this.tempUnit = builder.tempUnit;
            this.velocityUnit = builder.velocityUnit;
            this.areaUnit = builder.areaUnit;
            this.used = builder.used;
            this.agency = builder.agency;
            this.party = builder.party;
            this.wmComments = builder.wmComments;
            this.locationId = builder.locationId;
            this.officeId = builder.officeId;
            this.number = builder.number;
            this.instant = builder.instant;
            this.streamflowMeasurement = builder.streamflowMeasurement;
            this.supplementalStreamflowMeasurement = builder.supplementalStreamflowMeasurement;
            this.usgsMeasurement = builder.usgsMeasurement;
        }

        @JacksonXmlProperty(isAttribute = true)
        public String getHeightUnit() {
            return heightUnit;
        }

        @JacksonXmlProperty(isAttribute = true)
        public String getFlowUnit() {
            return flowUnit;
        }

        @JacksonXmlProperty(isAttribute = true)
        public String getTempUnit() {
            return tempUnit;
        }

        @JacksonXmlProperty(isAttribute = true)
        public String getVelocityUnit() {
            return velocityUnit;
        }

        @JacksonXmlProperty(isAttribute = true)
        public String getAreaUnit() {
            return areaUnit;
        }

        @JacksonXmlProperty(isAttribute = true)
        public boolean isUsed() {
            return used;
        }

        @JacksonXmlProperty(isAttribute = true)
        public String getOfficeId() {
            return officeId;
        }

        public String getAgency() {
            return agency;
        }

        public String getParty() {
            return party;
        }

        public String getWmComments() {
            return wmComments;
        }

        @JsonProperty("location")
        public String getLocationId() {
            return locationId;
        }

        @JsonProperty("date")
        public Instant getInstant() {
            return instant;
        }

        public String getNumber() {
            return number;
        }

        @JsonProperty("stream-flow-measurement")
        public StreamflowMeasurement getStreamflowMeasurement() {
            return streamflowMeasurement;
        }

        @JsonProperty("supplemental-stream-flow-measurement")
        public SupplementalStreamflowMeasurement getSupplementalStreamflowMeasurement() {
            return supplementalStreamflowMeasurement;
        }

        public UsgsMeasurement getUsgsMeasurement() {
            return usgsMeasurement;
        }

        @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
        static final class Builder {
            private String heightUnit;
            private String flowUnit;
            private String tempUnit;
            private String velocityUnit;
            private String areaUnit;
            private boolean used;
            private String agency;
            private String party;
            private String wmComments;
            private StreamflowMeasurement streamflowMeasurement;
            private SupplementalStreamflowMeasurement supplementalStreamflowMeasurement;
            private UsgsMeasurement usgsMeasurement;
            private String officeId;
            private String locationId;
            private Instant instant;
            private String number;

            Builder withHeightUnit(String heightUnit) {
                this.heightUnit = heightUnit;
                return this;
            }

            Builder withFlowUnit(String flowUnit) {
                this.flowUnit = flowUnit;
                return this;
            }

            Builder withTempUnit(String tempUnit) {
                this.tempUnit = tempUnit;
                return this;
            }

            Builder withVelocityUnit(String velocityUnit) {
                this.velocityUnit = velocityUnit;
                return this;
            }

            Builder withAreaUnit(String areaUnit) {
                this.areaUnit = areaUnit;
                return this;
            }

            Builder withUsed(boolean used) {
                this.used = used;
                return this;
            }

            Builder withAgency(String agency) {
                this.agency = agency;
                return this;
            }

            Builder withParty(String party) {
                this.party = party;
                return this;
            }

            Builder withWmComments(String wmComments) {
                this.wmComments = wmComments;
                return this;
            }

            @JsonProperty("stream-flow-measurement")
            Builder withStreamflowMeasurement(StreamflowMeasurement streamflowMeasurement) {
                this.streamflowMeasurement = streamflowMeasurement;
                return this;
            }

            @JsonProperty("supplemental-stream-flow-measurement")
            Builder withSupplementalStreamflowMeasurement(SupplementalStreamflowMeasurement supplementalStreamflowMeasurement) {
                this.supplementalStreamflowMeasurement = supplementalStreamflowMeasurement;
                return this;
            }

            Builder withUsgsMeasurement(UsgsMeasurement usgsMeasurement) {
                this.usgsMeasurement = usgsMeasurement;
                return this;
            }

            Builder withOfficeId(String officeId) {
                this.officeId = officeId;
                return this;
            }

            @JsonProperty("location")
            Builder withLocationId(String locationId) {
                this.locationId = locationId;
                return this;
            }

            Builder withNumber(String number) {
                this.number = number;
                return this;
            }

            @JsonProperty("date")
            Builder withInstant(Instant instant) {
                this.instant = instant;
                return this;
            }

            MeasurementXmlDto build() {
                return new MeasurementXmlDto(this);
            }
        }
    }
}
