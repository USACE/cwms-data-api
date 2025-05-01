import { UsaceBox, Badge, H3 } from "@usace/groundwork";
import { FaMapMarkerAlt, FaRulerVertical, FaCity } from "react-icons/fa";
import { MdLocationCity, MdPublic, MdOutlineAccessTime } from "react-icons/md";

export default function LocationCard({ location }) {
  if (!location) return null;

  const {
    officeId,
    name,
    publicName,
    description,
    timezoneName,
    latitude,
    longitude,
    elevation,
    elevationUnits,
    nearestCity,
    stateInitial,
    countyName,
    nation,
    locationType,
    locationKind,
    horizontalDatum,
    verticalDatum,
    mapLabel,
  } = location;

  return (
    <UsaceBox title={`Location: ${publicName || name}`}>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
        <div>
          <H3 className="text-blue-800 mb-2 flex items-center gap-2">
            <FaMapMarkerAlt /> {mapLabel || publicName}
          </H3>
          <p className="mb-1">
            <strong>Location ID:</strong> {name} ({officeId})
          </p>
          <p className="mb-1">
            <strong>Type:</strong> {locationType} ({locationKind})
          </p>
          <p className="mb-1">
            <strong>Description:</strong>{" "}
            {description && description !== "NULL" ? description : "N/A"}
          </p>
          <p className="mb-1 flex items-center gap-1">
            <MdOutlineAccessTime /> <strong>Time Zone:</strong> {timezoneName}
          </p>
        </div>

        <div>
          <H3 className="text-green-800 mb-2 flex items-center gap-2">
            <FaRulerVertical /> Elevation & Location
          </H3>
          <p className="mb-1">
            <strong>Elevation:</strong>{" "}
            {elevation ? `${elevation?.toFixed(2)} ${elevationUnits}` : "N/A"}
          </p>
          <p className="mb-1">
            <strong>Lat/Lon:</strong> {latitude?.toFixed(5)}, {longitude?.toFixed(5)}
          </p>
          <p className="mb-1">
            <strong>Horizontal Datum:</strong> {horizontalDatum} <br />
          </p>
          <p className="mb-1">
            <strong>Vertical Datum:</strong> {verticalDatum} <br />
            </p>
          <p className="mb-1 flex items-center gap-1">
            <MdLocationCity />
            <strong>Nearest City:</strong> {nearestCity}
          </p>
          <p className="mb-1 flex items-center gap-1">
            <MdPublic />
            <strong>Region:</strong> {countyName}, {stateInitial}, {nation}
          </p>
        </div>
      </div>

      <div className="mt-4 flex flex-wrap gap-2">
        <Badge color="blue">{locationKind}</Badge>
        <Badge color="green">{locationType}</Badge>
        <Badge color="gray">{horizontalDatum}</Badge>
        <Badge color="gray">{verticalDatum}</Badge>
        <Badge color="purple">{timezoneName}</Badge>
        <Badge color={location.active ? "green" : "red"}>
          {location.active ? "Active" : "Inactive"}
        </Badge>
      </div>
    </UsaceBox>
  );
}
