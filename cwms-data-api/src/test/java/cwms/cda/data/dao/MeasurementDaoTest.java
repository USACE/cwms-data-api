package cwms.cda.data.dao;

import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.measurement.Measurement;
import cwms.cda.data.dto.measurement.StreamflowMeasurement;
import cwms.cda.data.dto.measurement.SupplementalStreamflowMeasurement;
import cwms.cda.data.dto.measurement.UsgsMeasurement;
import cwms.cda.helpers.DTOMatch;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import org.apache.commons.io.IOUtils;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.STREAMFLOW_MEAS2_T;
import usace.cwms.db.jooq.codegen.udt.records.SUPP_STREAMFLOW_MEAS_T;

final class MeasurementDaoTest {
    private static final double DELTA = 0.0001;
    @Test
    void testFromJooqMeasurementRecord() {
        Instant instant = Instant.parse("2024-01-01T00:00:00Z");
        STREAMFLOW_MEAS2_T record = mock(STREAMFLOW_MEAS2_T.class);
        when(record.getLOCATION()).thenReturn(mockLocation());
        when(record.getMEAS_NUMBER()).thenReturn("12345");
        when(record.getAGENCY_ID()).thenReturn("USGS");
        when(record.getPARTY()).thenReturn("SomeParty");
        when(record.getDATE_TIME()).thenReturn(Timestamp.from(instant));
        when(record.getFLOW()).thenReturn(100.0);
        when(record.getGAGE_HEIGHT()).thenReturn(2.0);
        when(record.getQUALITY()).thenReturn("good");
        when(record.getWM_COMMENTS()).thenReturn("Test comment");
        when(record.getAREA_UNIT()).thenReturn("ft2");
        when(record.getFLOW_UNIT()).thenReturn("cfs");
        when(record.getHEIGHT_UNIT()).thenReturn("ft");
        when(record.getVELOCITY_UNIT()).thenReturn("fps");
        when(record.getTEMP_UNIT()).thenReturn("F");
        when(record.getUSED()).thenReturn(JooqDao.formatBool(true));
        when(record.getAIR_TEMP()).thenReturn(25.0);
        when(record.getCUR_RATING_NUM()).thenReturn("1");
        when(record.getCTRL_COND_ID()).thenReturn("UNSPECIFIED");
        when(record.getFLOW_ADJ_ID()).thenReturn("UNKNOWN");
        when(record.getDELTA_HEIGHT()).thenReturn(0.5);
        when(record.getDELTA_TIME()).thenReturn(60.0);
        when(record.getPCT_DIFF()).thenReturn(10.0);
        when(record.getREMARKS()).thenReturn("Some remarks");
        when(record.getSHIFT_USED()).thenReturn(11.0);
        when(record.getWATER_TEMP()).thenReturn(15.0);
        when(record.getSUPP_STREAMFLOW_MEAS()).thenReturn(mockSupplementalStreamflowMeasurement());

        Measurement measurement = MeasurementDao.fromJooqMeasurementRecord(record);

        assertNotNull(measurement);
        assertEquals("12345", measurement.getNumber());
        assertEquals("USGS", measurement.getAgency());
        assertEquals("SomeParty", measurement.getParty());
        assertEquals(instant, measurement.getInstant());
        assertEquals("Test comment", measurement.getWmComments());
        assertEquals("ft2", measurement.getAreaUnit());
        assertEquals("cfs", measurement.getFlowUnit());
        assertEquals("ft", measurement.getHeightUnit());
        assertEquals("fps", measurement.getVelocityUnit());
        assertEquals("F", measurement.getTempUnit());
        assertTrue(measurement.isUsed());

        //Assertions for Streamflow meas fields
        assertEquals(100.0, measurement.getStreamflowMeasurement().getFlow(), DELTA);
        assertEquals(2.0, measurement.getStreamflowMeasurement().getGageHeight(), DELTA);
        assertEquals("good", measurement.getStreamflowMeasurement().getQuality());

        // Assertions for UsgsMeasurement fields
        assertEquals(25.0, measurement.getUsgsMeasurement().getAirTemp(), DELTA);
        assertEquals("1", measurement.getUsgsMeasurement().getCurrentRating());
        assertEquals("UNSPECIFIED", measurement.getUsgsMeasurement().getControlCondition());
        assertEquals("UNKNOWN", measurement.getUsgsMeasurement().getFlowAdjustment());
        assertEquals(0.5, measurement.getUsgsMeasurement().getDeltaHeight(), DELTA);
        assertEquals(60.0, measurement.getUsgsMeasurement().getDeltaTime());
        assertEquals(10.0, measurement.getUsgsMeasurement().getPercentDifference(), DELTA);
        assertEquals("Some remarks", measurement.getUsgsMeasurement().getRemarks());
        assertEquals(11.0, measurement.getUsgsMeasurement().getShiftUsed(), DELTA);
        assertEquals(15.0, measurement.getUsgsMeasurement().getWaterTemp(), DELTA);

        // Assertions for SupplementalStreamflowMeasurement fields
        assertNotNull(measurement.getSupplementalStreamflowMeasurement());
        assertEquals(1.5, measurement.getSupplementalStreamflowMeasurement().getAvgVelocity(), DELTA);
        assertEquals(100.0, measurement.getSupplementalStreamflowMeasurement().getChannelFlow(), DELTA);
        assertEquals(3.0, measurement.getSupplementalStreamflowMeasurement().getMeanGage(), DELTA);
        assertEquals(2.0, measurement.getSupplementalStreamflowMeasurement().getMaxVelocity(), DELTA);
        assertEquals(50.0, measurement.getSupplementalStreamflowMeasurement().getOverbankFlow(), DELTA);
        assertEquals(200.0, measurement.getSupplementalStreamflowMeasurement().getOverbankArea(), DELTA);
        assertEquals(10.0, measurement.getSupplementalStreamflowMeasurement().getTopWidth(), DELTA);
        assertEquals(1.0, measurement.getSupplementalStreamflowMeasurement().getSurfaceVelocity(), DELTA);
        assertEquals(5.0, measurement.getSupplementalStreamflowMeasurement().getChannelMaxDepth(), DELTA);
        assertEquals(150.0, measurement.getSupplementalStreamflowMeasurement().getMainChannelArea(), DELTA);
        assertEquals(2.0, measurement.getSupplementalStreamflowMeasurement().getOverbankMaxDepth(), DELTA);
        assertEquals(75.0, measurement.getSupplementalStreamflowMeasurement().getEffectiveFlowArea(), DELTA);
        assertEquals(60.0, measurement.getSupplementalStreamflowMeasurement().getCrossSectionalArea(), DELTA);
    }

    @Test
    void testConvertToXmlMeasurementDto()
    {
        Measurement meas = buildTestMeasurement();

        MeasurementDao.MeasurementXmlDto xmlDto = MeasurementDao.convertMeasurementToXmlDto(meas);
        assertEquals(meas.getNumber(), xmlDto.getNumber());
        assertEquals(meas.getAgency(), xmlDto.getAgency());
        assertEquals(meas.getParty(), xmlDto.getParty());
        assertEquals(meas.getInstant(), xmlDto.getInstant());
        assertEquals(meas.getWmComments(), xmlDto.getWmComments());
        assertEquals(meas.getAreaUnit(), xmlDto.getAreaUnit());
        assertEquals(meas.getFlowUnit(), xmlDto.getFlowUnit());
        assertEquals(meas.getHeightUnit(), xmlDto.getHeightUnit());
        assertEquals(meas.getVelocityUnit(), xmlDto.getVelocityUnit());
        assertEquals(meas.getTempUnit(), xmlDto.getTempUnit());
        assertEquals(meas.isUsed(), xmlDto.isUsed());
        assertEquals(meas.getLocationId(), xmlDto.getLocationId());
        assertEquals(meas.getOfficeId(), xmlDto.getOfficeId());
        DTOMatch.assertMatch(meas.getStreamflowMeasurement(), xmlDto.getStreamflowMeasurement());
        DTOMatch.assertMatch(meas.getUsgsMeasurement(), xmlDto.getUsgsMeasurement());
        DTOMatch.assertMatch(meas.getSupplementalStreamflowMeasurement(), xmlDto.getSupplementalStreamflowMeasurement());
    }

    @Test
    void testToDbXml() throws Exception
    {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dao/dbMeasurement.xml");
        assertNotNull(resource);
        String expectedXml = IOUtils.toString(resource, StandardCharsets.UTF_8);
        MeasurementDao.MeasurementXmlDto expectedXmlDto = MeasurementDao.XML_MAPPER.readValue(expectedXml, MeasurementDao.MeasurementXmlDto.class);

        Measurement meas = buildTestMeasurement();
        String xml = MeasurementDao.toDbXml(meas);
        assertNotNull(xml);
        assertFalse(xml.isEmpty());
        MeasurementDao.MeasurementXmlDto actualXmlDto = MeasurementDao.XML_MAPPER.readValue(xml, MeasurementDao.MeasurementXmlDto.class);

        assertEquals(expectedXmlDto.getAgency(), actualXmlDto.getAgency());
        assertEquals(expectedXmlDto.getAreaUnit(), actualXmlDto.getAreaUnit());
        assertEquals(expectedXmlDto.getFlowUnit(), actualXmlDto.getFlowUnit());
        assertEquals(expectedXmlDto.getHeightUnit(), actualXmlDto.getHeightUnit());
        assertEquals(expectedXmlDto.getTempUnit(), actualXmlDto.getTempUnit());
        assertEquals(expectedXmlDto.getInstant(), actualXmlDto.getInstant());
        assertEquals(expectedXmlDto.getNumber(), actualXmlDto.getNumber());
        assertEquals(expectedXmlDto.getParty(), actualXmlDto.getParty());
        assertEquals(expectedXmlDto.getVelocityUnit(), actualXmlDto.getVelocityUnit());
        assertEquals(expectedXmlDto.getWmComments(), actualXmlDto.getWmComments());
        assertEquals(expectedXmlDto.getLocationId(), actualXmlDto.getLocationId());
        assertEquals(expectedXmlDto.getOfficeId(), actualXmlDto.getOfficeId());
        DTOMatch.assertMatch(expectedXmlDto.getStreamflowMeasurement(), actualXmlDto.getStreamflowMeasurement());
        DTOMatch.assertMatch(expectedXmlDto.getUsgsMeasurement(), actualXmlDto.getUsgsMeasurement());
        DTOMatch.assertMatch(expectedXmlDto.getSupplementalStreamflowMeasurement(), actualXmlDto.getSupplementalStreamflowMeasurement());
    }

    private Measurement buildTestMeasurement() {
        return new Measurement.Builder()
                .withNumber("12345")
                .withAgency("USGS")
                .withParty("SomeParty")
                .withInstant(Instant.parse("2024-01-01T00:00:00Z"))
                .withWmComments("Test comment")
                .withAreaUnit("ft2")
                .withFlowUnit("cfs")
                .withHeightUnit("ft")
                .withVelocityUnit("fps")
                .withTempUnit("F")
                .withUsed(true)
                .withId(new CwmsId.Builder()
                        .withName("Walnut_Ck")
                        .withOfficeId("SPK")
                        .build())
                .withStreamflowMeasurement(new StreamflowMeasurement.Builder()
                        .withFlow(100.0)
                        .withGageHeight(2.0)
                        .withQuality("good")
                        .build())
                .withUsgsMeasurement(new UsgsMeasurement.Builder()
                        .withAirTemp(25.0)
                        .withCurrentRating("1")
                        .withControlCondition("UNSPECIFIED")
                        .withFlowAdjustment("UNKNOWN")
                        .withDeltaHeight(0.5)
                        .withDeltaTime(60.0)
                        .withPercentDifference(10.0)
                        .withRemarks("Some remarks")
                        .withShiftUsed(11.0)
                        .withWaterTemp(15.0)
                        .build())
                .withSupplementalStreamflowMeasurement(new SupplementalStreamflowMeasurement.Builder()
                        .withAvgVelocity(1.5)
                        .withChannelFlow(100.0)
                        .withMeanGage(3.0)
                        .withMaxVelocity(2.0)
                        .withOverbankFlow(50.0)
                        .withOverbankArea(200.0)
                        .withTopWidth(10.0)
                        .withSurfaceVelocity(1.0)
                        .withChannelMaxDepth(5.0)
                        .withMainChannelArea(150.0)
                        .withOverbankMaxDepth(2.0)
                        .withEffectiveFlowArea(75.0)
                        .withCrossSectionalArea(60.0)
                        .build())
                .build();
    }

    private LOCATION_REF_T mockLocation() {
        LOCATION_REF_T location = new LOCATION_REF_T();
        location.setBASE_LOCATION_ID("Walnut_Ck");
        location.setSUB_LOCATION_ID(null);
        location.setOFFICE_ID("SPK");
        return location;
    }

    private SUPP_STREAMFLOW_MEAS_T mockSupplementalStreamflowMeasurement() {
        SUPP_STREAMFLOW_MEAS_T supplemental = new SUPP_STREAMFLOW_MEAS_T();
        supplemental.setAVG_VELOCITY(1.5);
        supplemental.setCHANNEL_FLOW(100.0);
        supplemental.setMEAN_GAGE(3.0);
        supplemental.setMAX_VELOCITY(2.0);
        supplemental.setOVERBANK_FLOW(50.0);
        supplemental.setOVERBANK_AREA(200.0);
        supplemental.setTOP_WIDTH(10.0);
        supplemental.setSURFACE_VELOCITY(1.0);
        supplemental.setCHANNEL_MAX_DEPTH(5.0);
        supplemental.setMAIN_CHANNEL_AREA(150.0);
        supplemental.setOVERBANK_MAX_DEPTH(2.0);
        supplemental.setEFFECTIVE_FLOW_AREA(75.0);
        supplemental.setCROSS_SECTIONAL_AREA(60.0);
        return supplemental;
    }
}
