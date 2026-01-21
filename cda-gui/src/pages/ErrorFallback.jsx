import { Button } from "@usace/groundwork";
import { FaArrowLeft } from "react-icons/fa";
import { Link } from "react-router-dom";

export default function ErrorFallback({ error }) {
  return (
    <div className="p-6">
      <h2 className="text-lg font-semibold text-red-600">Something went wrong</h2>
      <p className="mt-2 text-gray-700">
        {error?.message || "An unexpected error occurred."}
      </p>
      <p>
        <Link to="/">
          <Button
            size={"lg"}
            color={"dark"}
            className="mt-4 flex flex-row text-white font-bold py-3 px-6 rounded"
          >
            <FaArrowLeft className="mr-2" /> Go Home
          </Button>
        </Link>
      </p>
    </div>
  );
}
