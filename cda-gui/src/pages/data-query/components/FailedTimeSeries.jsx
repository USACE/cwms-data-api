import { Accordion, Badge, gwMerge } from "@usace/groundwork";
import { IoWarning } from "react-icons/io5";
import PropTypes from "prop-types";

export default function FailedTimeSeries({ failedTS, className }) {
  if (!failedTS || failedTS.length === 0) return null;

  //   TODO: fetch the extents if the timeseries error is not 404 to let the user know valid dates
  return (
    <div className={gwMerge("flex flex-col gap-2 mx-2 my-5", className)}>
      <Accordion
        heading={
          <div className="flex justify-between items-center w-full">
            <div className="text-xl font-bold">
              <IoWarning className="inline" /> Failed Timeseries
            </div>
            <Badge color="red">
              <b>{failedTS.length} Failed</b>
            </Badge>
          </div>
        }
      >
        <div className="py-3">
          {failedTS.map((tsid) => (
            <Badge key={"failed-" + tsid} color="yellow" className="ms-5">
              <b>{tsid}</b>
            </Badge>
          ))}
        </div>
      </Accordion>
    </div>
  );
}

FailedTimeSeries.propTypes = {
  failedTS: PropTypes.arrayOf(PropTypes.string),
  className: PropTypes.string,
};
