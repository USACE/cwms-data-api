import { Breadcrumbs as BC, BreadcrumbItem } from "@usace/groundwork";
import { getPathname, getBasePath } from "../utils/base";
import { useEffect } from "react";
import { useState } from "react";

export default function Breadcrumbs() {
  const [path, setPath] = useState(null);
  useEffect(() => {
    // Split the pathname and remove the empty strings and base path
    setPath(getPathname()
        .split("/")
        .filter(d=>d)
        .filter(d=>d != getBasePath().replace("/", ""))
    );
  }, []);
  // Wait for the path to be set before rendering
  if (!path) return null;
  return (
    <BC className="mt-0">
      {path.map((item, index) => <BreadcrumbItem key={index} href={item} text={item} />)}
    </BC>
  );
}
