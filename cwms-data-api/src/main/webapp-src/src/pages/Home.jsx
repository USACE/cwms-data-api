import { UsaceBox } from "@usace/groundwork";

function Home() {
  return (
    <>
      <UsaceBox title="Introduction">
        Welcome to the US Army Corps of Engineers Corps Water Management System
        Data API. At the Swagger UI link above you will find a REST styled
        interface for retrieving any data available publicly.
      </UsaceBox>
      <UsaceBox title="Some Quick Notes">
        <div>
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
