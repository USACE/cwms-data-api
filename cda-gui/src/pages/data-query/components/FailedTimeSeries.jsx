import { Accordion, Badge } from "@usace/groundwork";
import { IoWarning } from "react-icons/io5";

export default function FailedTimeSeries({ failedTS }) {
  if (!failedTS || failedTS.length === 0) return null;

 //   TODO: fetch the extents if the timeseries error is not 404 to let the user know valid dates
  return (
    <div className="flex flex-col gap-2 mx-2 my-5">
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
