import { Container, SiteWrapper, Button } from "@usace/groundwork";
import "@usace/groundwork/dist/style.css";
import links from "./nav-links";
import { useConnect } from "redux-bundler-hook";
import { getNavHelper } from "internal-nav-helper";
import { FaGithub } from "react-icons/fa";

// TODO: Convert the remaining swagger-ui.html and regexp.html to react pages
// TODO: Setup a build script that pushes files from dist to webapp

function App() {
  const { route: Route, doUpdateUrl } = useConnect(
    "selectRoute",
    "doUpdateUrl"
  );
  return (
    <div
      onClick={getNavHelper((url) => {
        doUpdateUrl(url);
      })}
    >
      <SiteWrapper
        links={links}
        usaBanner={true}
        subtitle={`CWMS Restful API for Data Retrieval`}
        aboutText="Deliver vital engineering solutions, in collaboration with our partners, to secure our Nation, energize our economy, and reduce disaster risk. The official public website of the U.S. Army Corps of Engineers Hydrologic Engineering Center (HEC). For website corrections, write to Webmaster-HEC@usace.army.mil."
        navRight={
          <>
            <Button
              missiontext="Corps Water Management System API"
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
        <Container className="mt-8">
          <Route />
        </Container>
      </SiteWrapper>
    </div>
  );
}

export default App;
