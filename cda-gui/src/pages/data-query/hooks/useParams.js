import { useQuery } from "@tanstack/react-query";
import { Configuration, ParametersApi } from "cwmsjs";

const config = new Configuration({
  basePath: import.meta.env.VITE_CDA_API_ROOT,
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
