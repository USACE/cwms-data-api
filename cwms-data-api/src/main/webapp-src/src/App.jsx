import { Container, SiteWrapper, Button } from "@usace/groundwork";
import "@usace/groundwork/dist/style.css";
import links from "./nav-links";
import { FaGithub } from "react-icons/fa";
import LoginButton from "./components/LoginButton";
import { getBasePath } from "./utils/base";
import {
  BrowserRouter,
  Routes,
  Route
} from "react-router-dom";
import Home from "./pages/Home";
import NotFound from "./pages/NotFound";
import SwaggerUI from "./pages/swagger-ui/index";
import Regexp from "./pages/regexp/index";
import Breadcrumbs from "./components/Breadcrumbs";


const BASE_PATH = getBasePath();

function App() {
  return (
    <div
    >
      <SiteWrapper
        links={links}
        usaBanner={true}
        subtitle={`CWMS Restful API for Data Retrieval`}
        aboutText="Deliver vital engineering solutions, in collaboration with our partners, to secure our Nation, energize our economy, and reduce disaster risk. The official public website of the U.S. Army Corps of Engineers Hydrologic Engineering Center (HEC). For website corrections, write to Webmaster-HEC@usace.army.mil."
        navRight={
          <>
            <LoginButton />
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
        <Container className="mt-2">
          <Breadcrumbs />
          <BrowserRouter basename={BASE_PATH}>
            <Routes>
              <Route path={`/`} element={<Home />} />
              <Route path={`/swagger-ui`} element={<SwaggerUI />} />
              <Route path={`/regexp`} element={<Regexp />} />
              <Route path="*" element={<NotFound />} />
            </Routes>
          </BrowserRouter>
        </Container>
      </SiteWrapper>
    </div>
  );
}

export default App;
