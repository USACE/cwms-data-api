import { useEffect } from "react";

function Decide() {
    useEffect(()=> {
        document.title = "CWMS Public Services";
    })
    return (
      <div>
        <p>
          Hello, you&apos;ve successfully reach the Corps Water Management
          System (CWMS) public data system. Multiple sites are available please
          follow one of the links below for access to our data:
        </p>
        <ul className="pl-5 list-disc list-outside [&_ul]:list-[revert]">
          <li>
            <a href="https://water.usace.army.mil">Access to Water</a> - This is
            our primary portal for information. A map based interface to learn
            about current flood control status
          </li>
          <li>
            <a href="/cwms-data/swagger-ui.html">CWMS DATA API (CDA)</a> - our
            RESTful API for Data Retrieval. If you are trying to acquire data
            for a project we suggest starting here.
          </li>
        </ul>
      </div>
    );
}

export default Decide;