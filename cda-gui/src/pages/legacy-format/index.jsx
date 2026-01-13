import { UsaceBox, H3 } from "@usace/groundwork";

export default function LegacyFormat() {
    return (
        <>
            <UsaceBox title="Legacy Format Responses">
                <p>
                    Several web service end-points make use of the provided accept header
                    to determine the response format. These differ based on whether the provided header
                    supports <code>JSON</code> or <code>JSONv2</code>, with the responses also differing based on
                    the use of the <code>format</code> query parameter.
                    These differences are caused by the usage of differing data retrieval methods.
                    The supported headers are listed below, along with the expected behavior for each:
                </p>

                <ul className="pl-5 list-disc">
                    <li><code>JSON</code> - <code className="text-sm bg-gray-100 px-2 py-1 rounded">application/json;version=1</code> -
                        Legacy format that utilizes PL/SQL methods (defined Oracle database procedures) for data retrieval.
                    </li>
                    <li><code>JSONv2</code> - <code className="text-sm bg-gray-100 px-2 py-1 rounded">application/json;version=2</code> -
                        Newer format utilizing SQL queries for data retrieval.
                        These generally have the newest formatting support.
                        Most endpoints will treat <code>application/json</code> or <code>*/*</code> as this format.
                    </li>
                </ul>
            </UsaceBox>

            <UsaceBox title="Combining Accept Header with Format Query Parameter">
                <p>
                    Multiple endpoints support using the accept header to specify the response format.
                    By default, providing a format parameter other than <code>JSON</code> will utilize
                    the legacy PL/SQL data access method. This generally provides more formats, but will result
                    in retrieving differing data content and structure depending on the specific endpoint.
                </p>
            </UsaceBox>

            <UsaceBox title="Accept Header Usage Examples">
                <p>
                    Below are a few examples of how to use the accept header to retrieve data in the desired format.
                </p>

                <H3 className="mt-4 font-semibold">
                    1. Using cURL
                </H3>
                <div className="ms-6">
                    <p>
                        Within a terminal that supports cURL usage, run a command such as the following:
                    </p>
                    <pre className="text-sm bg-gray-100 px-2 py-1 rounded">
                        {`curl --location 'https://cwms-data.usace.army.mil/cwms-data/locations?FORMAT=json' \\ \n--header 'Accept: application/json;version=1'`}
                    </pre>
                </div>

                <H3 className="mt-4 font-semibold">
                    2. Using wGET
                </H3>
                <div className="ms-6">
                    <p>
                        Within a terminal that supports wGET usage, run a command such as the following:
                    </p>
                    <pre className="text-sm bg-gray-100 px-2 py-1 rounded">
                        {`wget \\ \n--method GET \\ \n--header 'Accept: application/json;version=1' \\ \n'https://cwms-data.usace.army.mil/cwms-data/locations?FORMAT=json'`}
                    </pre>
                </div>

                <H3 className="mt-4 font-semibold">
                    3. Using Python
                </H3>
                <div className="ms-6">
                    <p>
                        Within a Python shell or script file, use the following to query with an accept header:
                    </p>
                    <pre className="text-sm bg-gray-100 px-2 py-1 rounded">
                        {`import requests \nx = requests.get("http://cwms-data.usace.army.mil/cwms-data/locations?OFFICE=SPK&FORMAT=json", headers={"Accept": "application/json"}) \nprint(x.json())`}
                    </pre>
                </div>
            </UsaceBox>
        </>
    );
}