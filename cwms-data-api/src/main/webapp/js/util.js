function load(id, url) {
  var con = document.getElementById(id);
  var xhr = new XMLHttpRequest();

  xhr.onreadystatechange = function () {
    // Check if the request is complete and was successful
    if (xhr.readyState == 4 && xhr.status == 200) {
      con.innerHTML = xhr.responseText;

      // Reinitialize any scripts in the loaded content
      // This is necessary because innerHTML does not execute scripts
      // We create new script elements and replace the old ones
      // to ensure they are executed.
      const scripts = con.querySelectorAll("script");
      scripts.forEach((oldScript) => {
        const newScript = document.createElement("script");

        // Add attributes from the old script to the new one
        Array.from(oldScript.attributes).forEach(attr =>
          newScript.setAttribute(attr.name, attr.value)
        );

        // If the old script has inline code, set it as the text content of the new script
        if (oldScript.innerHTML) {
          newScript.textContent = oldScript.innerHTML;
        }

        // Replace the old script with the new one
        oldScript.parentNode.replaceChild(newScript, oldScript);
      });
    }
    else if (xhr.readyState == 4) {
      console.error("Error loading content from " + url + ": " + xhr.statusText);
      con.innerHTML = "<p>Error loading content. Please try again later.</p>";
    }
  };

  xhr.open("GET", url, true);
  xhr.setRequestHeader("Content-type", "text/html");
  xhr.send();
}
