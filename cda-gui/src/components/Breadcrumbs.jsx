import { Breadcrumbs as BC, BreadcrumbItem } from "@usace/groundwork";
import { useLocation } from "react-router-dom";

// Replace hyphens with spaces and capitalize each word
function formatSegment(segment) {
  return segment
    .replace(/-/g, " ")
    .replace(/\b\w/g, char => char.toUpperCase());
}

export default function Breadcrumbs() {
  const location = useLocation();
  const basePath = import.meta.env.BASE_URL?.replace(/\/$/, "") || "";
  const fullPath = location.pathname.replace(basePath, "");

  const pathSegments = fullPath.split("/").filter(Boolean);

  if (pathSegments.length === 0) return null; // Hide on root

  // combine by base paths and format each segment
  const breadcrumbs = pathSegments.map((segment, index) => {
    const href = "/" + pathSegments.slice(0, index + 1).join("/");
    return {
      text: formatSegment(segment),
      href,
    };
  });

  return (
    <BC className="ms-5 my-0">
      {breadcrumbs.map(({ text, href }, i) => (
        <BreadcrumbItem key={i} href={href} text={text} />
      ))}
    </BC>
  );
}
