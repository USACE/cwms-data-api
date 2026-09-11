import { Outlet } from "react-router-dom";
import { Container, SiteWrapper, Button } from "@usace/groundwork";
import headerLinks from "../links/header-links";
import footerLinks from "../links/footer-links";
import externalLinks from "../links/external-links";
import Breadcrumbs from "./Breadcrumbs";
import { FaGithub } from "react-icons/fa";
import AuthButton from "./AuthButton";
import { useAuth } from "@usace-watermanagement/groundwork-water";

export default function Layout() {
  const { isAuth } = useAuth();
  return (
    <SiteWrapper
      links={headerLinks.filter((link) => !link.requiresAuth || isAuth)}
      usaBanner
      army250Logo
      subtitle="CWMS Restful API for Data Retrieval"
      aboutText="Deliver vital engineering solutions, in collaboration with our partners, to secure our Nation, energize our economy, and reduce disaster risk. The official public website of the U.S. Army Corps of Engineers Hydrologic Engineering Center (HEC)."
      navRight={
        <div className="flex items-center gap-2">
          <AuthButton />
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
        </div>
      }
      usaceLinks={footerLinks}
      externalLinks={externalLinks}
      facebookUrl="http://www.facebook.com/USACEHQ"
      twitterUrl="http://twitter.com/USACEHQ"
      youtubeUrl="http://www.youtube.com/CORPSCONNECTION"
      flickrUrl="http://www.flickr.com/photos/usacehq"
      linkedInUrl="https://www.linkedin.com/company/us-army-corps-of-engineers/posts/?feedView=all"
    >
      <Container className="mt-2">
        <Breadcrumbs />
        <Outlet />
      </Container>
    </SiteWrapper>
  );
}
