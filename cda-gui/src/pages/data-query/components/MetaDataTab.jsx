import { Skeleton } from "@usace/groundwork";
import { useCdaLocation } from "@usace-watermanagement/groundwork-water";
import LocationCard from "./LocationCard";
import PropTypes from "prop-types";

export default function MetaDataTab({ office, tsids }) {
  const metaData = useCdaLocation({
    cdaParams: {
      locationId: tsids[0].split(".")[0],
      office,
    },
    queryOptions: {
      enabled: !!tsids && !!office && !!tsids.length > 0,
      refetchOnWindowFocus: false,
      refetchOnMount: false,
      refetchOnReconnect: false,
      retry: 1,
      keepPreviousData: true,
      staleTime: Infinity,
      cacheTime: Infinity,
    },
  });

  if (metaData.isLoading) return <Skeleton type="card" className="w-full h-[500px]" />;
  return <LocationCard location={metaData.data} />;
}

MetaDataTab.propTypes = {
  office: PropTypes.string.isRequired,
  tsids: PropTypes.arrayOf(PropTypes.string).isRequired,
};
