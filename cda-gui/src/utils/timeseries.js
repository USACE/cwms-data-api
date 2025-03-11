/**
 * Get the index of the time series ID with the smallest interval.
 *
 * @param {string[]} tsids - An array of time series identifiers.
 * @returns {number} The index of the ID with the smallest interval.
 */
export function getMinInterval(tsids) {
  // Map time intervals to a comparable value in minutes
  const timeMap = {
    Minutes: 1,
    Hour: 60,
    Day: 1440, //  24 * 60
    Week: 10080, //   7 * 24 * 60
    Month: 43200, //  30 * 1440
    Year: 525600, // 365 * 24 * 60
  };

  // Helper function to parse the time interval from a string
  function parseInterval(str) {
    const timeString = str.split(".")[3]; // Get the 3rd part
    const timeValue = timeString.replace("~", "").replace(/[^0-9]/g, ""); // Extract the number
    const timeUnit = timeString.replace(/[^a-zA-Z]/g, ""); // Extract the unit (Minutes, Hours, Day, Month)

    return {
      value: timeValue ? parseInt(timeValue) : 1, // Fallback to 1 if the timeValue is empty
      unit: timeUnit,
    };
  }

  let intervalCompare = Infinity;
  let minValue = "";
  let minInterval = Infinity;

  // Iterate over each value in the array
  let tsIdx;
  for (let i = 0; i < tsids.length; i++) {
    const { value, unit } = parseInterval(tsids[i]);
    const intervalInMinutes = value * (timeMap[unit] || Infinity); // Convert to minutes

    // Check for the minimum interval
    if (intervalInMinutes < intervalCompare) {
      intervalCompare = intervalInMinutes;
      minValue = unit;
      minInterval = value;
      tsIdx = i;
    }
  }
  return tsIdx;
}

/**
 * Merges a list of timeseries objects into a single consolidated timeseries object.
 * The function combines the values of each timeseries based on their timestamps (epochs)
 * and stores them in a merged object. Timeseries that fail (i.e., have no values) are recorded separately.
 *
 * @function mergeTimeseries
 * @param {Array<Object>} timeseriesList - List of timeseries objects to be merged.
 * Each timeseries object should have the following properties:
 *   @param {string} timeseriesList[].name - The name or identifier of the timeseries.
 *   @param {Array<Array<number|string>>} timeseriesList[].values - An array of values for the timeseries.
 *   Each value should be an array containing [epoch, value, quality_code].
 *   @param {string} timeseriesList[].units - The unit of measurement for the timeseries values.
 * 
 * @returns {Object} A merged timeseries object with the following properties:
 *   @property {Array<string>} tsids - List of timeseries identifiers that were successfully merged.
 *   @property {Object} values - An object where each key is an epoch (timestamp), and the value is an array of merged values for that timestamp.
 *   @property {Array<number>} dates - An array of unique epochs (timestamps) present in the merged timeseries.
 *   @property {Array<string>} failed - List of timeseries identifiers that failed to merge (i.e., had no values).
 */
export function mergeTimeseries(timeseriesList) {
  const merged = { tsids: [], values: {}, dates: [], failed: []};
  timeseriesList.forEach((ts) => {
    if (ts?.values.length) {
      merged.tsids.push({ name: ts.name, units: ts.units });
      ts.values.forEach((v) => {
        // destructure value array [epoch, value, quality_code]
        let [_d, _v, _q] = v;
        // Parse the value to a float with the correct precision given the units
        _v = parseFloat(_v) || null;
        if (!merged.values[_d]) {
          // Create a new array for the epoch
          merged.values[_d] = [_v];
          merged.dates.push(_d);
        } else {
            merged.values[_d].push(_v);
        }
      });
    } else {
      merged.failed.push(ts.name);
    }
  });
  return merged;
}

/**
 * Determines the decimal precision for a given unit of measurement.
 *
 * This function takes a unit of measurement as input and returns the number of decimal places
 * that should be used when displaying values for that unit. If the unit is not recognized, a default precision is returned.
 *
 * @function getPrecision
 * @param {string} units - The unit of measurement to determine the precision for (e.g., "ft", "%", "cfs").
 *   The function is case insensitive.
 *
 * @returns {number} The number of decimal places to be used for the specified unit:
 *   - Returns `2` for "ft", "%", "in", and any unrecognized units.
 *   - Returns `1` for "irrad", "langley/min", "mph", "f", "deg", and "volt".
 *   - Returns `0` for "cfs" and "ac-ft".
 */
export const getPrecision = (units) => {
  let unit = units?.toLowerCase() || ""; // empty string if units are undefined or null
  switch (unit) {
    case "ft":
    case "%":
    case "in":
      return 2;
    case "irrad":
    case "langley/min":
    case "mph":
    case "f":
    case "deg":
    case "volt":
      return 1;
    case "cfs":
    case "ac-ft":
      return 0;
    default:
      return 2; // Default precision
  }
};

/**
 * Retrieves the latest non-null value from a timeseries data object.
 *
 * The function iterates through the `values` array of the provided data object in reverse order,
 * looking for the most recent entry that has a non-null value. The `values` array should be a 2D array,
 * where each element is in the format `[timestamp, value, quality_code]`.
 *
 * @function getLatestValue
 * @param {Object} data - The timeseries data object containing the values to search through.
 * @param {Array<Array<number|null>>} data.values - A 2D array where each element is an array representing a timestamp, value, and quality code:
 *      - `timestamp` {number}: The epoch time of the value.
 *      - `value` {number|null}: The value at the given timestamp. May be `null` if no value is recorded.
 *      - `quality_code` {number}: A code representing the quality of the value.
 *
 * @returns {Array<number|null>|null} The latest `[timestamp, value, quality_code]` array where `value` is not `null`.
 * Returns `null` if no valid value is found.
 */
export function getLatestValue(data) {
  // values is a 2D array of [[timestamp, value, quality_code], ...]
  for (let index = data?.values.length - 1; index > 0; index--) {
    const value = data.values[index];
    if (value[1] != null) return value;
  }
  return null;
}
