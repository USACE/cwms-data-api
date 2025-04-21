/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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

import static java.util.stream.Collectors.toList;

import cwms.cda.api.errors.RateException;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.rating.RateInput;
import cwms.cda.data.dto.rating.RateInputTimeSeries;
import cwms.cda.data.dto.rating.RateInputValues;
import cwms.cda.data.dto.rating.RatedOutput;
import cwms.cda.data.dto.rating.RatedOutputTimeSeries;
import cwms.cda.data.dto.rating.RatedOutputValues;
import hec.data.cwmsRating.RatingSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.jooq.ConnectionCallable;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import usace.cwms.db.jooq.codegen.packages.CWMS_RATING_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.DATE_TABLE_TYPE;
import usace.cwms.db.jooq.codegen.udt.records.DOUBLE_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.DOUBLE_TAB_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.STR_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.ZTSV_ARRAY;

public class RateDao extends JooqDao<RatingSet> {


    public RateDao(DSLContext dsl) {
        super(dsl);
    }

    public RatedOutput rate(String officeId, String ratingId, RateInputValues input) {
        DOUBLE_TAB_T outputValues = connectionResult(c -> {
            DSLContext context = getDslContext(c, officeId);
            DATE_TABLE_TYPE ratingDates = null;
            if (!input.getValueTimes().isEmpty()) {

                ratingDates = new DATE_TABLE_TYPE();
                input.getValueTimes().stream().map(Timestamp::new).forEach(ratingDates::add);
            }
            DOUBLE_TAB_TAB_T inputValues = new DOUBLE_TAB_TAB_T();
            input.getValues().stream().map(DOUBLE_TAB_T::new).forEach(inputValues::add);
            STR_TAB_T unitsTab = new STR_TAB_T(input.getInputUnits());
            unitsTab.add(input.getOutputUnit());
            return CWMS_RATING_PACKAGE.call_RATE(context.configuration(), ratingId,
                inputValues, unitsTab, formatBool(input.getRound()), ratingDates, null, "UTC", officeId);
        });
        return new RatedOutputValues(CwmsId.buildCwmsId(officeId, ratingId), outputValues, input.getOutputUnit());
    }

    private void validateReverseRateInput(RateInput input) {
        if(input instanceof RateInputTimeSeries && ((RateInputTimeSeries) input).getTimeSeriesIds().size() > 1) {
            throw new IllegalArgumentException("Reverse Rating only supports one dependent parameter");
        }
        if(input instanceof RateInputValues) {
            List<List<Double>> values = ((RateInputValues) input).getValues();
            if(values.size() > 1) {
                throw new IllegalArgumentException("Reverse Rating only supports one time series at a time");
            }
            List<Double> inputValues = new ArrayList<>(values.get(0));
            Collections.sort(inputValues);
            for (int i = 1; i < inputValues.size(); i++) {
                if(inputValues.get(i) == null) {
                    throw new IllegalArgumentException("Input values must be non-null");
                }
                if (inputValues.get(i) < inputValues.get(i - 1)) {
                    throw new IllegalArgumentException("Input values must be monotonically increasing/decreasing");
                }
            }
        }
    }

    public RatedOutput reverseRate(String officeId, String ratingId, RateInputValues input) {
        validateReverseRateInput(input);
        DOUBLE_TAB_T outputValues = connectionResult(c -> {
            DSLContext context = getDslContext(c, officeId);
            DATE_TABLE_TYPE ratingDates = null;
            if (!input.getValueTimes().isEmpty()) {

                ratingDates = new DATE_TABLE_TYPE();
                input.getValueTimes().stream().map(Timestamp::new).forEach(ratingDates::add);
            }
            DOUBLE_TAB_T inputValues = new DOUBLE_TAB_T(input.getValues().get(0));
            STR_TAB_T unitsTab = new STR_TAB_T(input.getInputUnits());
            unitsTab.add(input.getOutputUnit());
            return CWMS_RATING_PACKAGE.call_REVERSE_RATE(context.configuration(), ratingId,
                inputValues, unitsTab, formatBool(input.getRound()), ratingDates, null, "UTC", officeId);
        });
        return new RatedOutputValues(CwmsId.buildCwmsId(officeId, ratingId), outputValues, input.getOutputUnit());
    }

    public RatedOutput rate(String officeId, String ratingId, RateInputTimeSeries input) {
        ZTSV_ARRAY ztsvTypes = connectionResult(c -> {
            DSLContext context = getDslContext(c, officeId);
            Timestamp version = input.getVersionDate().map(Timestamp::from).orElse(null);
            Timestamp ratingTimstamp = input.getRatingTime().map(Timestamp::from).orElse(null);
            STR_TAB_T independentTsIds = new STR_TAB_T(input.getTimeSeriesIds());
            return CWMS_RATING_PACKAGE.call_RETRIEVE_RATED_TS(context.configuration(), independentTsIds, ratingId,
                input.getOutputUnit(), Timestamp.from(input.getStartTime()), Timestamp.from(input.getEndTime()),
                ratingTimstamp, "UTC", formatBool(input.getRound()), formatBool(input.getTrim()),
                formatBool(input.getStartInclusive()), formatBool(input.getEndInclusive()),
                formatBool(input.getPrevious()), formatBool(input.getNext()), version, "T", officeId,
                officeId);
        });
        List<TimeSeries.Record> records = ztsvTypes.stream()
            .map(z -> new TimeSeries.Record(z.getDATE_TIME(), z.getVALUE(), z.getQUALITY_CODE().intValue()))
            .collect(toList());
        return new RatedOutputTimeSeries(CwmsId.buildCwmsId(officeId, ratingId), records, input.getOutputUnit());
    }

    public RatedOutput reverseRate(String officeId, String ratingId, RateInputTimeSeries input) {
        //Performing early simple validation in order to avoid validation within the database
        validateReverseRateInput(input);
        ZTSV_ARRAY ztsvTypes = connectionResult(c -> {
            DSLContext context = getDslContext(c, officeId);
            Timestamp version = input.getVersionDate().map(Timestamp::from).orElse(null);
            Timestamp ratingTimstamp = input.getRatingTime().map(Timestamp::from).orElse(null);
            String timeseriesId = input.getTimeSeriesIds().get(0);
            return CWMS_RATING_PACKAGE.call_RETRIEVE_REVERSE_RATED_TS(context.configuration(), timeseriesId, ratingId,
                input.getOutputUnit(), Timestamp.from(input.getStartTime()), Timestamp.from(input.getEndTime()),
                ratingTimstamp, "UTC", formatBool(input.getRound()), formatBool(input.getTrim()),
                formatBool(input.getStartInclusive()), formatBool(input.getEndInclusive()),
                formatBool(input.getPrevious()), formatBool(input.getNext()), version, "T", officeId,
                officeId);
        });
        List<TimeSeries.Record> records = ztsvTypes.stream()
            .map(z -> new TimeSeries.Record(z.getDATE_TIME(), z.getVALUE(), z.getQUALITY_CODE().intValue()))
            .collect(toList());
        return new RatedOutputTimeSeries(CwmsId.buildCwmsId(officeId, ratingId), records, input.getOutputUnit());
    }

    private <R> R connectionResult(ConnectionCallable<R> callable) {
        try {
            return connectionResult(dsl, callable);
        } catch(DataAccessException ex) {
            throw handleRateDbError(ex);
        }
    }

    static RuntimeException handleRateDbError(DataAccessException ex) {
        RuntimeException retval = ex;
        Throwable cause = ex.getCause();
        if (cause instanceof SQLException) {
            int errorCode = ((SQLException) cause).getErrorCode();
            if (errorCode == 20019 || errorCode == 20998) {
                String localizedMessage = cause.getLocalizedMessage();
                String[] parts = localizedMessage.split("\n");
                String message = parts[0];
                int index = message.indexOf(":");
                if (index >= 0) {
                    retval = new RateException(message.substring(index + 1), (SQLException) cause);
                }
            }
        }
        return retval;
    }
}
