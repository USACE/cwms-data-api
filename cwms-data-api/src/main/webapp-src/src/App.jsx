import { 
    Breadcrumbs, 
    BreadcrumbItem, 
    Container, 
    SiteWrapper, 
    Button, 
    UsaceBox 
} from "@usace/groundwork";
import "@usace/groundwork/dist/style.css"
import links from "./nav-links";
import { FaGithub } from "react-icons/fa";

function App() {
  return (
    <SiteWrapper
      links={links}
      usaBanner={true}
      subtitle={`CWMS Restful API for Data Retrieval`}
      aboutText="Deliver vital engineering solutions, in collaboration with our partners, to secure our Nation, energize our economy, and reduce disaster risk. The official public website of the U.S. Army Corps of Engineers Hydrologic Engineering Center (HEC). For website corrections, write to Webmaster-HEC@usace.army.mil."
      navRight={
        <>
          <Button
            missionText="Corps Water Management System API"
            style="plain"
            color="white"
            size="lg"
            href="https://github.com/USACE/cwms-data-api"
            title="View on GitHub"
          >
            GitHub <FaGithub />
          </Button>
        </>
      }
    >
      <Container className="gw-py-3">
        <UsaceBox title="Introduction">
          Welcome to the US Army Corps of Engineers Corps Water Management
          System Data API. At the Swagger UI link above you will find a REST
          styled interface for retrieving any data available publicly.
        </UsaceBox>
        <UsaceBox title="Some Quick Notes">
          <div>
            <ul>
              <li>
                Dates and Intervals/Durations are always in either the ISO 8601
                or milliseconds since the unix epoch.
              </li>
              <li>
                The interval is the time between two different measurements.
                <ul>
                  <li>
                    An interval of 0 and irregular are equivalent and mean that
                    there is no fixed interval between measurements
                  </li>
                </ul>
              </li>
              <li>
                The Duration is used for aggregating calculations (Sums over
                time, Averages) and indicates the time window over which samples
                are gathered to generate a value.
              </li>
              <li>Errors are always returned as JSON objects</li>
            </ul>
          </div>
        </UsaceBox>
      </Container>
    </SiteWrapper>
  );
}

export default App;

