/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import { UsaceBox, Button, Input, H3 } from "@usace/groundwork";
import { useState } from "react";
import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { Configuration, OfficesApi, CatalogApi } from "cwmsjs";

const offices_api = new OfficesApi(
  new Configuration({
    basePath: import.meta.env.VITE_CDA_API_ROOT,
  }),
);
const catalog_api = new CatalogApi(
  new Configuration({
    basePath: import.meta.env.VITE_CDA_API_ROOT,
    headers: { accept: "application/json;version=2" },
  }),
);

export default function LocationSearch() {
  const [office, setOffice] = useState("");
  const [searchText, setSearchText] = useState("");
  const [unitSystem, setUnitSystem] = useState("EN");
  const [like, setLike] = useState("");
  const [locationCategoryLike, setLocationCategoryLike] = useState("");
  const [locationGroupLike, setLocationGroupLike] = useState("");
  const [boundingOfficeLike, setBoundingOfficeLike] = useState("");
  const [locationKindLike, setLocationKindLike] = useState("");
  const [locationTypeLike, setLocationTypeLike] = useState("");
  const [triggerSearch, setTriggerSearch] = useState(false);
  const [searchCount, setSearchCount] = useState(0);

  const buildCatalogParams = (page) => {
    const params = {
      dataset: "LOCATIONS",
      office,
    };

    if (page) params.page = page;
    if (searchText.trim()) params.searchText = searchText.trim();
    if (unitSystem) params.unitSystem = unitSystem;
    if (like.trim()) params.like = like.trim();
    if (locationCategoryLike.trim())
      params.locationCategoryLike = locationCategoryLike.trim();
    if (locationGroupLike.trim()) params.locationGroupLike = locationGroupLike.trim();
    if (boundingOfficeLike.trim())
      params.boundingOfficeLike = boundingOfficeLike.trim();
    if (locationKindLike.trim()) params.locationKindLike = locationKindLike.trim();
    if (locationTypeLike.trim()) params.locationTypeLike = locationTypeLike.trim();

    return params;
  };

  const offices = useQuery({
    queryKey: ["offices"],
    queryFn: async () => {
      const entries = await offices_api.getOffices({
        hasData: true,
      });
      return [...new Set(entries.map((e) => e.name))];
    },
    retry: 1,
    staleTime: 1000 * 60 * 60 * 24,
  });

  const searchResults = useInfiniteQuery({
    queryKey: [
      "location-search",
      searchCount,
      office,
      searchText,
      unitSystem,
      like,
      locationCategoryLike,
      locationGroupLike,
      boundingOfficeLike,
      locationKindLike,
      locationTypeLike,
    ],
    queryFn: ({ pageParam }) =>
      catalog_api.getCatalogWithDataset(buildCatalogParams(pageParam)),
    initialPageParam: undefined,
    getNextPageParam: (lastPage) => lastPage.nextPage || undefined,
    enabled: triggerSearch && !!office && searchCount > 0,
    retry: 1,
  });

  const allResults = searchResults.data?.pages.flatMap((page) => page.entries) ?? [];
  const nextPage = searchResults.hasNextPage;

  const handleSearch = () => {
    setTriggerSearch(true);
    setSearchCount((prev) => prev + 1);
  };

  const handleLoadMore = () => {
    if (!searchResults.hasNextPage || searchResults.isFetchingNextPage) return;
    searchResults.fetchNextPage();
  };

  if (offices.isLoading) return <div>Loading offices...</div>;

  return (
    <div className="px-5">
      <UsaceBox title="Location Text Search Demo">
        <div className="mb-4 flex gap-4">
          <div className="flex flex-col min-w-[150px]">
            <label htmlFor="office" className="text-sm font-medium mb-1">
              Select Office
            </label>
            <select
              id="office"
              value={office}
              onChange={(e) => {
                setOffice(e.target.value);
                setTriggerSearch(false);
              }}
              className="px-3 py-1 rounded border border-gray-300"
            >
              <option key="select" value="">
                Select Office
              </option>
              {offices.data?.map((key) => (
                <option key={key} value={key}>
                  {key}
                </option>
              ))}
            </select>
          </div>
          <div className="flex flex-col min-w-[150px]">
            <label htmlFor="unitSystem" className="text-sm font-medium mb-1">
              Unit System
            </label>
            <select
              id="unitSystem"
              value={unitSystem}
              onChange={(e) => setUnitSystem(e.target.value)}
              className="px-3 py-1 rounded border border-gray-300"
            >
              <option value="EN">EN</option>
              <option value="SI">SI</option>
            </select>
          </div>
        </div>
        {office && (
          <>
            <div className="mb-4 grid grid-cols-1 md:grid-cols-2 gap-4">
              <Input
                label="Like (regex)"
                value={like}
                onChange={(e) => setLike(e.target.value)}
                placeholder="Regex for location ID"
              />
              <Input
                label="Location Category Like"
                value={locationCategoryLike}
                onChange={(e) => setLocationCategoryLike(e.target.value)}
                placeholder="Category filter"
              />
              <Input
                label="Location Group Like"
                value={locationGroupLike}
                onChange={(e) => setLocationGroupLike(e.target.value)}
                placeholder="Group filter"
              />
              <Input
                label="Bounding Office Like"
                value={boundingOfficeLike}
                onChange={(e) => setBoundingOfficeLike(e.target.value)}
                placeholder="Bounding office filter"
              />
              <Input
                label="Location Kind Like"
                value={locationKindLike}
                onChange={(e) => setLocationKindLike(e.target.value)}
                placeholder="Kind filter"
              />
              <Input
                label="Location Type Like"
                value={locationTypeLike}
                onChange={(e) => setLocationTypeLike(e.target.value)}
                placeholder="Type filter"
              />
            </div>
            <div className="mb-4">
              <Input
                label="Search Text"
                value={searchText}
                onChange={(e) => setSearchText(e.target.value)}
                placeholder="Enter text to search in location metadata"
              />
              <Button
                onClick={handleSearch}
                disabled={searchResults.isLoading}
                className="mt-2"
              >
                {searchResults.isLoading ? "Searching..." : "Search"}
              </Button>
            </div>
          </>
        )}
        {searchResults.isLoading && allResults.length === 0 && (
          <div>Loading results...</div>
        )}
        {searchResults.isLoading && allResults.length > 0 && (
          <div className="mb-2 text-sm text-gray-600">Refreshing results...</div>
        )}
        {searchResults.error && <div>Error: {searchResults.error.message}</div>}
        {allResults.length > 0 && (
          <div
            className={
              searchResults.isLoading
                ? "opacity-60 transition-opacity"
                : "opacity-100 transition-opacity"
            }
          >
            <H3>Search Results ({allResults.length})</H3>
            <div className="overflow-x-auto border border-gray-300 rounded">
              <table className="w-full border-collapse border border-gray-300">
                <thead>
                  <tr className="bg-gray-100">
                    <th className="border border-gray-300 px-2 py-1 text-left">Name</th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Public Name
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Long Name
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Description
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">Kind</th>
                    <th className="border border-gray-300 px-2 py-1 text-left">Type</th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Time Zone
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Nearest City
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Latitude
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Longitude
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Published Latitude
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Published Longitude
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Horizontal Datum
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Elevation
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">Unit</th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Vertical Datum
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Nation
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      State
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      County
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Bounding Office
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Map Label
                    </th>
                    <th className="border border-gray-300 px-2 py-1 text-left">
                      Active
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {allResults.map((loc) => (
                    <tr key={loc.name} className="hover:bg-gray-50">
                      <td className="border border-gray-300 px-2 py-1">{loc.name}</td>
                      <td className="border border-gray-300 px-2 py-1">
                        {loc.publicName}
                      </td>
                      <td className="border border-gray-300 px-2 py-1">
                        {loc.longName}
                      </td>
                      <td className="border border-gray-300 px-2 py-1">
                        {loc.description}
                      </td>
                      <td className="border border-gray-300 px-2 py-1">{loc.kind}</td>
                      <td className="border border-gray-300 px-2 py-1">{loc.type}</td>
                      <td className="border border-gray-300 px-2 py-1">
                        {loc.timeZone}
                      </td>
                      <td className="border border-gray-300 px-2 py-1">
                        {loc.nearestCity}
                      </td>
                      <td className="border border-gray-300 px-2 py-1">
                        {loc.latitude}
                      </td>
                      <td className="border border-gray-300 px-2 py-1">
                        {loc.longitude}
                      </td>
                      <td className="border border-gray-300 px-2 py-1">
                        {loc.publishedLatitude}
                      </td>
                      <td className="border border-gray-300 px-2 py-1">
                        {loc.publishedLongitude}
                      </td>
                      <td className="border border-gray-300 px-2 py-1">
                        {loc.horizontalDatum}
                      </td>
                      <td className="border border-gray-300 px-2 py-1">
                        {loc.elevation}
                      </td>
                      <td className="border border-gray-300 px-2 py-1">{loc.unit}</td>
                      <td className="border border-gray-300 px-2 py-1">
                        {loc.verticalDatum}
                      </td>
                      <td className="border border-gray-300 px-2 py-1">{loc.nation}</td>
                      <td className="border border-gray-300 px-2 py-1">{loc.state}</td>
                      <td className="border border-gray-300 px-2 py-1">{loc.county}</td>
                      <td className="border border-gray-300 px-2 py-1">
                        {loc.boundingOffice}
                      </td>
                      <td className="border border-gray-300 px-2 py-1">
                        {loc.mapLabel}
                      </td>
                      <td className="border border-gray-300 px-2 py-1">
                        {loc.active ? "Yes" : "No"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {nextPage && (
                <Button
                  onClick={handleLoadMore}
                  disabled={searchResults.isFetchingNextPage}
                  className="mt-4"
                >
                  {searchResults.isFetchingNextPage ? "Loading..." : "Load More"}
                </Button>
              )}
            </div>
          </div>
        )}
        {allResults.length === 0 && triggerSearch && !searchResults.isLoading && (
          <div>No locations found matching the criteria.</div>
        )}
      </UsaceBox>
    </div>
  );
}
