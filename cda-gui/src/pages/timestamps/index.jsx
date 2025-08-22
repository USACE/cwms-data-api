import { UsaceBox, Code, Divider, H3, H4 } from "@usace/groundwork";

export default function Timestamps() {
  return (
    <>
      <UsaceBox title="Timestamps">
        <p>
          Several web service end-points make use of timestamp fields to
          represent start and end times. The CWMS Data API provides flexible
          date/time parsing and supports several formats:
        </p>

        <ul className="pl-5 list-disc">
          <li>Full ISO 8601 dates with timezone information</li>
          <li>
            ISO 8601 dates without timezone information (using the fallback
            timezone)
          </li>
          <li>Period strings (starting with {'"P"'})</li>
          <li>Duration strings (starting with {'"PT"'})</li>
        </ul>

        <p>
          There are still a few web-service end-points that pass user-provided
          timestamp string parameters directly to pl/sql functions. Those
          end-points are sometimes described as legacy or version 1 end-points.
          They often take a {'"format"'} parameter and do not use the accept
          header. In those cases the pl/sql parses the user string and a
          slightly different timestamp format must be used.
        </p>
      </UsaceBox>

      <UsaceBox className="mt-1" title="Filter Expression Language Examples">
        <p>
          The following are examples of acceptable input strings for date/time
          parameters:
        </p>

        <H3 className="mt-4 font-semibold">
          1. ISO 8601 Dates with Timezone Information
        </H3>
        <div className="ms-6">
          <p>
            These formats include timezone information, so the fallback timezone
            is ignored:
          </p>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 w-1/2">
            <div className="flex flex-col my-2">
              <code className="text-sm bg-gray-100 px-2 py-1 rounded">
                2022-01-06T00:00:00Z
              </code>
              <span className="text-gray-700">UTC timezone (Z)</span>
            </div>

            <div className="flex flex-col my-2">
              <code className="text-sm bg-gray-100 px-2 py-1 rounded">
                2022-01-06T07:00:01-07:00
              </code>
              <span className="text-gray-700">Offset timezone (-07:00)</span>
            </div>

            <div className="flex flex-col my-2">
              <code className="text-sm bg-gray-100 px-2 py-1 rounded">
                2022-01-06T07:00:02-0700
              </code>
              <span className="text-gray-700">
                Offset timezone without colon
              </span>
            </div>

            <div className="flex flex-col my-2">
              <code className="text-sm bg-gray-100 px-2 py-1 rounded">
                2021-06-10T13:00:23-07:00[PST8PDT]
              </code>
              <span className="text-gray-700">Offset with named timezone</span>
            </div>

            <div className="flex flex-col my-2">
              <code className="text-sm bg-gray-100 px-2 py-1 rounded">
                2022-01-19T20:52:07+00:00[UTC]
              </code>
              <span className="text-gray-700">
                Zero offset with named timezone
              </span>
            </div>

            <div className="flex flex-col my-2">
              <code className="text-sm bg-gray-100 px-2 py-1 rounded">
                2022-12-08T09:47:32-0800[America/Los_Angeles]
              </code>
              <span className="text-gray-700">Offset with region timezone</span>
            </div>
          </div>
        </div>

        <H3 className="mt-4 font-semibold">
          2. ISO 8601 Dates without Timezone Information
        </H3>
        <div className="ms-6">
          <p>
            When timezone information is not included, the fallback timezone is
            used:
          </p>
          <ul className="pl-5 list-disc">
            <li>
              <code>2022-01-06T00:00:01</code> - Will be interpreted in the
              fallback timezone
            </li>
            <li>
              <code>2021-04-05</code> - Date only (time will be set to midnight
              in the fallback timezone)
            </li>
          </ul>
          <p>
            For example, if the fallback timezone is {'"US/Pacific"'} and you
            provide <code>2022-01-06T00:00:00</code>, it will be interpreted as
            January 6, 2022, at midnight Pacific time.
          </p>
        </div>
        <H3 className="mt-4 font-semibold">
          3. Period Strings (Calendar-based)
        </H3>
        <div className="ms-6">
          <p>
            Period strings start with {'"P"'} and represent calendar-based
            periods (days, months, years). They are relative to the current
            time:
          </p>
          <ul className="pl-5 list-disc">
            <li>
              <code>P-1D</code> - 1 day ago
            </li>
            <li>
              <code>P-1M</code> - 1 month ago
            </li>
            <li>
              <code>P-1Y</code> - 1 year ago
            </li>
            <li>
              <code>P-1Y-2M-3D</code> - 1 year, 2 months, and 3 days ago
            </li>
          </ul>
          <p>
            Note: Periods respect calendar boundaries. For example, one month
            ago from March 30 is February 28 (in non-leap years).
          </p>
        </div>

        <H3 className="mt-4 font-semibold">4. Duration Strings (Time-based)</H3>
        <div className="ms-6">
          <p>
            Duration strings start with {'"PT"'} and represent time-based
            durations (hours, minutes, seconds). They are relative to the
            current time:
          </p>
          <ul className="pl-5 list-disc">
            <li>
              <code>PT-24H</code> - 24 hours ago
            </li>
            <li>
              <code>PT-25H-3M</code> - 25 hours and 3 minutes ago
            </li>
            <li>
              <code>PT-1H-30M</code> - 1 hour and 30 minutes ago
            </li>
          </ul>
          <p>
            Note: Durations are exact time intervals. PT-24H is always exactly
            24 hours, regardless of daylight saving time changes.
          </p>
        </div>
        <Divider />

        <H3 className="mt-4 font-semibold">Timezone Handling</H3>
        <p>The API handles timezones in the following ways:</p>
        <ul className="pl-5 list-disc">
          <li>
            If the input string includes timezone information, that timezone is
            used regardless of the fallback timezone.
          </li>
          <li>
            If the input string does not include timezone information, the
            fallback timezone is used.
          </li>
          <li>
            The fallback timezone comes from the {'"timezone"'} query parameter
            or is specified in the API call.
          </li>
          <li>
            If no fallback timezone is specified, UTC is used as the default.
          </li>
        </ul>

        <H3 className="mt-4 font-semibold">Error Handling</H3>
        <p>
          The API will throw a <Code>DateTimeException</Code> in the following
          cases:
        </p>
        <ul className="pl-5 list-disc">
          <li>
            The input string cannot be parsed as a valid date/time, period, or
            duration.
          </li>
          <li>The specified fallback timezone is invalid.</li>
        </ul>
      </UsaceBox>
    </>
  );
}
