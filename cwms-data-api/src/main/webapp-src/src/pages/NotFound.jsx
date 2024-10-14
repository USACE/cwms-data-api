import { UsaceBox, Button } from "@usace/groundwork";
import { Link } from "react-router-dom";
import { FaArrowLeft } from "react-icons/fa";

function NotFound() {
return (
  <>
    <UsaceBox title="404: Not Found">
      <p className="text-xl">The page you are looking for does not exist.</p>
      <Link to="./">
        <Button
          size={"lg"}
          color={"dark"}
          className="mt-4 flex flex-row text-white font-bold py-3 px-6 rounded"
        >
          <FaArrowLeft className="mr-2" /> Go Home
        </Button>
      </Link>
    </UsaceBox>
  </>
);
}

export default NotFound;
