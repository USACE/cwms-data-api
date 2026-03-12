import { Configuration, AuthorizationApi } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

test("Test Raw", async () => {
  // There is no public auth setup yet.
  // The following is for districts to write their data to the their district box.

  // This assumes you have a reverse proxy setup to /district-data from the tomcat port and CDA with auth enabled
  const config = new Configuration({
    basePath: "https://your-t7-address-for-auth/district-data",
    headers: {
      accept: "application/json;version=2",
    },
  });
  // Tell cwmsjs to use our internal server and version 2 of any endpoints
  const a_api = new AuthorizationApi(config);

  // Every method has another sister method with Raw at the end.
  // These methods return the raw response object from the fetch call.
  // This enables you to do things like : read the status code, headers, etc
  // And make decisions if you need off of these values
  //  i.e. if a 401 Unauthorized response is given, you can tell the user to login again

  async function isLoggedIn() {
    // Utility function that returns if a user is logged in based on if they can get the keys for their user (protected endpoint)
    return await a_api
      .getDataAuthKeysRaw()
      .then((r) => {
        if (r.raw.ok) return r.raw.json();
        else if (r.raw.status == 401) {
          // redirect to the login page
          return false;
        }
      })
      .then((d) => {
        // Print the response to the console, just so you can see that
        console.log(d);
        return true;
      });
  }
  // You should use a 'useState' hook to store the logged_in state and update the UI based on that if using React
  // JS Module means to doing this:
  const logged_in = await isLoggedIn();
  if (!logged_in) alert("You must login first! 401 Unauthorized");
  else alert("You are logged in!");
  // Then update the state of your app based on if it is logged in
}, 60000);
