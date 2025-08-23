import { UsaceBox } from "@usace/groundwork";
import { Link } from "react-router-dom";

function Home() {
  return (
    <>
      <UsaceBox title="Introduction" className="mt-8">
        Welcome to the US Army Corps of Engineers Corps Water Management System
        Data API.
      </UsaceBox>
      <UsaceBox title="Quick Links">
        <ul className="ms-5 pl-5 list-disc list-outside [&_ul]:list-[revert]">
          <li>
            <Link className="underline" to="/swagger-ui">
              Swagger UI
            </Link>{" "}
            - Interactive API documentation
          </li>
          <li>
            <Link className="underline" to="/data-query">
              Data Query Tool
            </Link>{" "}
            - A tool for querying and retrieving data from the API.
          </li>
          <li>
            <Link className="underline" to="/regexp">
              Regular Expressions
            </Link>{" "}
            - A guide to using regular expressions in the API.
          </li>
        </ul>
      </UsaceBox>
      <UsaceBox title="Some Quick Notes">
        <div>
          <Link to="/swagger-ui" className="underline">
            Swagger UI
          </Link>
          <ul className="pl-5 list-disc list-outside [&_ul]:list-[revert]">
            <li>
              Dates and Intervals/Durations are always in either the ISO 8601 or
              milliseconds since the unix epoch.
            </li>
            <li>
              The interval is the time between two different measurements.
              <ul className="list-disc ml-4">
                <li>
                  An interval of 0 and irregular are equivalent and mean that
                  there is no fixed interval between measurements
                </li>
              </ul>
            </li>
            <li>
              The Duration is used for aggregating calculations (Sums over time,
              Averages) and indicates the time window over which samples are
              gathered to generate a value.
            </li>
            <li>Errors are always returned as JSON objects</li>
          </ul>
        </div>
      </UsaceBox>
    </>
  );
}

export default Home;
