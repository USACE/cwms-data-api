import { useQuery } from "@tanstack/react-query";
import { Configuration, OfficesApi } from "cwmsjs";

const CDA_URL =
  import.meta.env.VITE_CDA_URL ||
  (import.meta.env.BASE_URL || "").replace(/\/$/, "") ||
  "/cwms-data";
const config = new Configuration({
  basePath: CDA_URL,
});
const officesApi = new OfficesApi(config);

export default function useOffices({ cacheDuration, props }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["offices"],
    queryFn: async () => officesApi.getOffices(),
    staleTime: cacheDuration,
    ...props,
  });

  return { data, isLoading, error };
}
