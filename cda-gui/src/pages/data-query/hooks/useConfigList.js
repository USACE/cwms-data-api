import { useQuery } from "@tanstack/react-query";
import { BlobApi, Configuration } from "cwmsjs";

const CDA_URL = import.meta.env.CDA_URL;
const config = new Configuration({
  basePath: CDA_URL,
});
const blobApi = new BlobApi(config);

export default function useConfigList({ cacheDuration, props, like }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["offices"],
    queryFn: async () =>
      blobApi.getBlobs({
        like,
      }),
    staleTime: cacheDuration,
    ...props,
  });

  return { data, isLoading, error };
}
