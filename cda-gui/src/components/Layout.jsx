import { Outlet } from "react-router-dom";
import { Container, SiteWrapper, Button } from "@usace/groundwork";
import links from "../nav-links";
import Breadcrumbs from "./Breadcrumbs";
import { FaGithub } from "react-icons/fa";

export default function Layout() {
  return (
    <SiteWrapper
      links={links}
      usaBanner={true}
      subtitle={`CWMS Restful API for Data Retrieval`}
      aboutText="Deliver vital engineering solutions, in collaboration with our partners, to secure our Nation..."
      navRight={
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
      }
    >
      <Container className="mt-2">
        <Breadcrumbs />
        <Outlet />
      </Container>
    </SiteWrapper>
  );
}
