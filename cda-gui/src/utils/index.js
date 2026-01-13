// Utility function to debounce - add a delay to a function call
function debounce(func, wait) {
  let timeout;
  return function (...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => func.apply(this, args), wait);
  };
}
const capitalize = (s) => s.charAt(0).toUpperCase() + s.slice(1);

/**
 * Creates a list of integers given a start and end index
 * @constructor
 * @param {integer} start_value - Number to Start From.
 * @param {integer} end_value - Number to End From.
 * @returns {list[integer]}
 * @description Creates a list of integers given a start and end index
 */
function range(start_value = 0, end_value) {
  // takes a start value and an end value and returns a list of integers between them
  // Will take them in either order
  let values = [];
  if (start_value < end_value)
    for (let x = start_value; x < end_value; x++) {
      values.push(x);
    }
  else
    for (let x = end_value; x >= start_value; x--) {
      values.push(x);
    }
  return values;
}

export { debounce, capitalize, range };
