import { useQuery } from "@tanstack/react-query";
import { Configuration, ParametersApi } from "cwmsjs";

const CDA_URL = import.meta.env.CDA_URL;
const config = new Configuration({
  basePath: CDA_URL,
});
const paramsApi = new ParametersApi(config);

export default function useParams({ office, cacheDuration, props }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["params", office],
    queryFn: async () => paramsApi.getParameters({ office }),
    staleTime: cacheDuration,
    ...props,
  });

  return { data, isLoading, error };
}
