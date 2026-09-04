import { UsaceBox } from "@usace/groundwork";
import { Link } from "react-router-dom";
import PropTypes from "prop-types";
import { examples, exampleUrl, publicApi } from "./examples";
import "./quick-start.css";

function RequestExample({ name, title }) {
  const example = examples[name];
  const url = exampleUrl(example);
  return (
    <details className="qs-request">
      <summary>{title}</summary>
      <p>
        GET <code>{example.path}</code>
      </p>
      <dl className="qs-parameters">
        {Object.entries(example.params).map(([key, value]) => (
          <div key={key}>
            <dt>
              <code>{key}</code>
            </dt>
            <dd>
              <code>{value}</code>
            </dd>
          </div>
        ))}
      </dl>
      <p>
        <a href={url} target="_blank" rel="noreferrer">
          Open public API example
        </a>
        . Your browser may display an older JSON layout. For the version 2 layout
        described here, use this command in a terminal (use <code>curl.exe</code> in
        Windows PowerShell):
      </p>
      <pre>
        <code>{`curl --fail-with-body -H "Accept: application/json;version=2" "${url}"`}</code>
      </pre>
    </details>
  );
}
RequestExample.propTypes = {
  name: PropTypes.string.isRequired,
  title: PropTypes.string.isRequired,
};

const screenshots = {
  home: {
    file: "public-home.jpg",
    width: 1265,
    height: 712,
    alt: "Public CDA home page. Highlight 1 marks Data Query Tool under Quick Links. Highlight 2 marks API Docs for developers.",
    marks: [
      [48, 359, 126, 28, "1"],
      [162, 37, 86, 38, "2"],
    ],
  },
  query: {
    file: "public-data-query.jpg",
    width: 1265,
    height: 1000,
    alt: "Public Data Query with SWT selected. Numbered highlights identify the office, time series, date range, and results.",
    marks: [
      [136, 286, 164, 40, "1"],
      [171, 344, 544, 56, "2"],
      [99, 431, 582, 73, "3"],
      [30, 508, 286, 51, "4"],
    ],
  },
  locations: {
    file: "local-location-search.jpg",
    width: 1536,
    height: 850,
    alt: "Local preview using public SWT data. Highlights mark the office, the exact KEYS location ID filter, and Search.",
    marks: [
      [30, 257, 162, 46, "1"],
      [30, 307, 736, 48, "2"],
      [30, 531, 90, 50, "3"],
    ],
  },
};

function Screenshot({ name }) {
  const shot = screenshots[name];
  return (
    <figure id={`screenshot-${name}`} className="qs-figure">
      <div className="qs-image">
        <img
          src={`${import.meta.env.BASE_URL.replace(/\/$/, "")}/quick-start/${shot.file}`}
          alt={shot.alt}
          width={shot.width}
          height={shot.height}
          loading="lazy"
        />
        {shot.marks.map(([x, y, width, height, label]) => (
          <span
            key={label}
            aria-hidden="true"
            className="qs-highlight"
            style={{
              left: `${(100 * x) / shot.width}%`,
              top: `${(100 * y) / shot.height}%`,
              width: `${(100 * width) / shot.width}%`,
              height: `${(100 * height) / shot.height}%`,
            }}
          >
            <b>{label}</b>
          </span>
        ))}
      </div>
      <figcaption>{shot.alt}</figcaption>
    </figure>
  );
}
Screenshot.propTypes = { name: PropTypes.string.isRequired };

export default function QuickStart() {
  return (
    <article className="quick-start">
      <header className="qs-intro">
        <p className="qs-eyebrow">CWMS DATA API · GETTING STARTED</p>
        <h1>Find and download water data</h1>
        <p>
          Start with a USACE office, find a location, and collect time series and
          reference levels. These examples use Tulsa District (<code>SWT</code>) and
          Keystone Lake (<code>KEYS</code>).
        </p>
        <div className="qs-actions">
          <Link to="/data-query">Open Data Query</Link>
          <Link to="/location-search">Open Location Search</Link>
        </div>
      </header>

      <nav aria-label="On this page" className="qs-contents">
        <strong>On this page</strong>
        <a href="#before-you-start">Before you start</a>
        <a href="#choose-office">1. Choose an office</a>
        <a href="#find-locations">2. Find locations</a>
        <a href="#collect-timeseries">3. Collect time series</a>
        <a href="#collect-levels">4. Collect levels</a>
        <a href="#read-results">Read the results</a>
        <a href="#get-help">Get help</a>
      </nav>

      <section id="before-you-start">
        <UsaceBox title="Before you start">
          <p>
            <strong>Data is preliminary and subject to change.</strong> Automated
            collection and processing systems can produce delayed, missing, or incorrect
            values. Data may be revised after review. A successful request does not mean
            that every value has been verified.
          </p>
          <p>
            These preliminary data are for general information only and shall not be
            used in studies, designs, or other technical applications. Obtain and verify
            critical data with the responsible USACE office. The United States
            Government assumes no liability for completeness or accuracy. Read the{" "}
            <Link to="/disclaimer">USACE water-data disclaimer</Link> and the{" "}
            <Link to="/disclaimer#external-links">external link disclaimer</Link>.
          </p>
          <p>
            For a chart or table, start with <Link to="/data-query">Data Query</Link>.
            To find a site by its name or description, use{" "}
            <Link to="/location-search">Location Search</Link>.{" "}
            <Link to="/swagger-ui">Swagger UI</Link> is intended for developers: it
            documents endpoint parameters, response formats, and API operations. You do
            not need Swagger to use the browser tools.
          </p>
          <Screenshot name="home" />
        </UsaceBox>
      </section>

      <section id="choose-office">
        <UsaceBox title="1. Choose the office that owns the data">
          <p>
            Office codes identify the USACE office publishing a record. Select
            <strong> SWT</strong> in the tools or include <code>office=SWT</code>
            in a request. For another district, choose its office and discover its
            identifiers again; location names and available series vary by office.
          </p>
          <p>
            The owning office and the office whose geographic boundary contains a site
            can differ. Use the office returned with the catalog record. Public CDA
            exposes the data made available there; a missing entry does not prove that
            an office has no such data.
          </p>
          <RequestExample name="offices" title="List offices with data" />
          <h3>Discover first, then request a record</h3>
          <div className="qs-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>You want</th>
                  <th>Catalog or listing</th>
                  <th>Retrieve</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <th>Locations</th>
                  <td>
                    <code>/catalog/LOCATIONS</code>
                  </td>
                  <td>
                    <code>/locations/&#123;location-id&#125;</code>
                  </td>
                </tr>
                <tr>
                  <th>Time series</th>
                  <td>
                    <code>/catalog/TIMESERIES</code>
                  </td>
                  <td>
                    <code>/timeseries?name=...</code>
                  </td>
                </tr>
                <tr>
                  <th>Levels</th>
                  <td>
                    <code>/levels</code>
                  </td>
                  <td>
                    <code>/levels/&#123;level-id&#125;/timeseries</code>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <p>
            A catalog lists available identifiers and metadata; it does not return the
            observations. Levels have their own listing at <code>/levels</code>; there
            is no <code>/catalog/LEVELS</code> endpoint.
          </p>
          <p>
            All API examples below use the public service at{" "}
            <a href={`${publicApi}/`}>{publicApi}</a>. They are read-only requests and
            do not require a login. The dated examples use September 1–2, 2026; replace
            the dates for your own request.
          </p>
        </UsaceBox>
      </section>

      <section id="find-locations">
        <UsaceBox title="2. Find a location">
          <ol>
            <li>
              Open <Link to="/location-search">Location Search</Link> from Tools, or use
              the link here.
            </li>
            <li>
              Select <strong>SWT</strong>, enter <strong>Keystone</strong> in Search
              Text, then select <strong>Search</strong>.
            </li>
            <li>
              Find <code>KEYS</code> / Keystone Lake. Keep the location ID and its
              office for the next requests.
            </li>
          </ol>
          <p>
            If Location Search is unavailable on your CDA installation, use the location
            catalog request below. In the catalog, <code>like</code> is a regular
            expression: <code>^KEYS</code> finds IDs starting with KEYS;
            <code>^KEYS$</code> matches only KEYS.
          </p>
          <p>
            Older API versions may not support Search Text. If your search returns
            unrelated locations, clear Search Text, enter <code>^KEYS$</code> in the
            field with the placeholder “Regex for location ID”, and select Search. This
            is the lookup shown below.
          </p>
          <Screenshot name="locations" />
          <RequestExample name="locations" title="Browse the SWT location catalog" />
          <RequestExample name="location" title="Get Keystone Lake location details" />
          <p>
            Location details describe the site, including coordinates, time zone, and
            vertical datum. The location record’s <code>elevation</code> is site
            metadata, not the current lake level. Use a time series for observations.
          </p>
        </UsaceBox>
      </section>

      <section id="collect-timeseries">
        <UsaceBox title="3. Collect time-series observations">
          <ol>
            <li>
              Open <Link to="/data-query">Data Query</Link> and select{" "}
              <strong>SWT</strong>.
            </li>
            <li>
              Switch to Manual Mode if the tool shows separate location and parameter
              selectors. Search the time-series selector for <code>KEYS.Elev</code> and
              choose <code>KEYS.Elev.Inst.1Hour.0.Ccp-Rev</code>.
            </li>
            <li>
              Choose a begin and end date within the series’ available period. Start
              with one day.
            </li>
            <li>
              Review the chart or table, check the units, and use the download control
              to save the data.
            </li>
          </ol>
          <Screenshot name="query" />
          <RequestExample name="catalog" title="Browse the SWT time-series catalog" />
          <p>
            Copy the full <code>name</code> from the catalog. Check its units and
            <code> extents</code> (earliest and latest available times) before choosing
            dates. A forecast series is different from an observed series, even when
            both describe the same location and parameter.
          </p>
          <p>
            The six parts of <code>KEYS.Elev.Inst.1Hour.0.Ccp-Rev</code> are location,
            parameter, parameter type, interval, duration, and version.
            <code> Elev</code> is elevation; <code>Inst</code> is instantaneous;
            <code> 1Hour</code> is the measurement interval; <code>0</code> means no
            aggregation duration. Keep the version exactly as listed; naming conventions
            and preferred series vary by office.
          </p>
          <RequestExample
            name="timeseries"
            title="Get one day of Keystone elevation observations"
          />
        </UsaceBox>
      </section>

      <section id="collect-levels">
        <UsaceBox title="4. Collect reference levels as a time series">
          <p>
            A location level defines a named reference, such as Top of Conservation or
            Top of Flood Control. It can be constant, seasonal, or change with its
            effective date. It is different from the measured water elevation.
          </p>
          <RequestExample
            name="levels"
            title="Browse the SWT levels listing (the levels catalog)"
          />
          <p>
            Copy a complete <code>location-level-id</code>, such as
            <code> KEYS.Elev.Inst.0.Top of Conservation</code>. The levels listing uses{" "}
            <code>level-id-mask</code> with <code>*</code> wildcards, not the catalog’s
            regular-expression <code>like</code> filter. It may contain multiple
            effective dates for the same name; do not assume the first constant value
            applies to your requested dates.
          </p>
          <RequestExample
            name="level"
            title="Evaluate Top of Conservation over the requested dates"
          />
          <p>
            Use <code>/levels/&#123;level-id&#125;/timeseries</code> with
            <code> office=SWT</code>, <code>unit=ft</code>, a begin and end time, and
            <code> interval=1Day</code>. Encode spaces in the level ID as
            <code>%20</code>, as the example does. The endpoint returns the level’s
            values at the requested time step, accounting for the applicable level
            definition. Request <code>1Hour</code> if you need hourly reference values.
          </p>
          <p>
            This interval controls the output sampling; it does not turn a reference
            level into an observation. A constant level will repeat across timestamps.
            Use the same dates, compatible units, and the same vertical datum before
            comparing it with observed elevations. Unit conversion alone does not
            convert a vertical datum.
          </p>
        </UsaceBox>
      </section>

      <section id="read-results">
        <UsaceBox title="Read and save the results">
          <ul>
            <li>
              <strong>Dates:</strong> API examples use ISO 8601 timestamps with{" "}
              <code>Z</code> for UTC. Browser date controls may use local time; check
              the displayed time zone. See <Link to="/timestamps">Timestamps</Link>.
            </li>
            <li>
              <strong>Values:</strong> version 2 time-series responses describe each
              column in <code>value-columns</code>. Typically each <code>values</code>{" "}
              row contains a Unix timestamp in milliseconds, a value, and a quality
              code. Preserve the quality code and missing values; missing is not zero.
            </li>
            <li>
              <strong>Units:</strong> read the returned units. The examples request{" "}
              <code>ft</code> for elevation values and <code>EN</code> for
              location/level listings. Other parameters need appropriate units.
            </li>
            <li>
              <strong>More pages:</strong> when a listing or time-series response
              includes <code>next-page</code>, send that exact token as the URL-encoded{" "}
              <code>page</code> parameter with the same filters. Continue until no
              next-page remains. A page-size limit is not the total number of records.
            </li>
            <li>
              <strong>Saving:</strong> download a table from Data Query, or add{" "}
              <code>-o data.json</code> to the curl command. Keep the office,
              identifier, units, date range, time zone, quality information, and
              retrieval date with your saved data.
            </li>
          </ul>
          <p>
            No results? Check the office, full identifier, catalog extents, filters, and
            dates. A catalog entry can exist without values in your chosen window. For a
            failed request, read the error message and try a small date range. For
            repeated downloads, reuse catalogs and avoid repeatedly requesting an
            office’s full history.
          </p>
        </UsaceBox>
      </section>

      <section id="get-help">
        <UsaceBox title="Questions, issues, and feature requests">
          <p>
            Ask the responsible office about data availability, unexplained values, or
            which series to use. For API parameters and integration details, see
            <Link to="/swagger-ui"> Swagger UI for developers</Link>.
          </p>
          <p>
            Found a bug, an unclear instruction, or a feature you would like?
            <a href="https://github.com/USACE/cwms-data-api/issues">
              {" "}
              Open or search a GitHub issue
            </a>
            . Include the office, location or full series/level ID, date range and time
            zone, request URL, and what you expected versus what happened. Remove
            credentials and other private information before posting.
          </p>
        </UsaceBox>
      </section>
    </article>
  );
}
