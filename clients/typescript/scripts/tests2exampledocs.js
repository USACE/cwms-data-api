// To run
// npm run examples
// This script will generate example documentation from the tests in the tests/endpoints directory
// Notes:
// Adding //!ignore anywhere in a file will cause it to get ignored
const fs = require("fs");
const path = require("path");
const prettier = require("prettier");

const prettierOptions = {
  parser: "babel",
  tabWidth: 4,
  singleQuote: true,
  trailingComma: "es5",
};

const testDirectory = "./tests/endpoints";
const genDirectory = "./tests/generator";
const outputDirectory = "./cwmsjs/docs/examples";
const templatePath = "./scripts/exampletemplate.html"; // Path to your HTML template
const packageVersion = JSON.parse(
  fs.readFileSync("./cwmsjs/package.json", "utf8")
).version;

// Ensure the output directory exists
if (!fs.existsSync(outputDirectory)) {
  fs.mkdirSync(outputDirectory, { recursive: true });
}
function getFiles(directories) {
  // Combine all the test files across directories we care about
  let all_files = [];
  directories.forEach((d) => {
    all_files = fs
      .readdirSync(d)
      .filter((fileName) => fileName.endsWith(".test.js"))
      .map((fileName) => path.join(d, fileName))
      .concat(all_files);
  });
  return all_files.sort();
}
// Read the HTML template
fs.readFile(templatePath, "utf8", (err, template) => {
  if (err) throw err;
  let all_files = getFiles([genDirectory, testDirectory]);
  all_files.forEach((filePath) => {
    // const filePath = path.join(testDirectory, file);
    fs.readFile(filePath, "utf8", (err, content) => {
      if (content.includes("//!ignore")) {
        console.log(`\tIgnoring ${path.basename(filePath)}`);
        return;
      }
      if (err) throw err;
      // Pull the content of the jest test block via regex
      const extractBlock = content.match(
        /test\(.*?\)\s*=>\s*{([\s\S]*?)},\s*\d+\s*\)/
      );
      // Ensure there was anything returned / i.e. this was a jest test file
      if (extractBlock && extractBlock[1]) {
        console.log(extractBlock[0]);
        // Strip out the bits we do not need from the file
        let block = extractBlock[1]
          .replaceAll(/expect\((.*?)\)\.toBeDefined\(\)/g, "console.log($1)") // Convert expects to logs
          .replaceAll("await ", "") // Let users decide when to add await
          .trim();

        // Adjust indentation (simple left trim here, more sophisticated methods might be needed)
        block = block
          .split("\n")
          .map((line) => line.replace(/^ {4}/, ""))
          .join("\n");

        const docName = path.basename(filePath).replace(".test.js", "");
        const docFullName = docName
          .replaceAll("-", " ")
          .replace(".v", " - Version ");

        // Build final documentation content with preserved imports
        const imports = content.match(/import.*;/g) || [];
        const combinedImports = imports
          .join("\n")
          .replace(/^\s*import fetch.*\n?/gm, "");

        prettier.format(block, prettierOptions).then((formattedBlock) => {
          console.log(formattedBlock);

          // Replace placeholder in template
          const filledTemplate = template
            .replaceAll("${docName}", docFullName)
            .replaceAll("${packageVersion}", packageVersion)
            .replaceAll(
              "${pageBody}",
              `
<h2>React + Vite Example</h2>
<b>To Install:</b>
<code class="language-shell">npm install cwmsjs --save</code><br>
<pre>
<code class="language-javascript">${combinedImports}\n\n${formattedBlock}</code>
</pre>
<br />
<hr />
<h2>Bundle / Vanilla JS Example</h2>
<b>To Install:</b><br>
<p>
<ol>
<li>Run <br>
<code class="language-shell">curl -O "https://raw.githubusercontent.com/HydrologicEngineeringCenter/cwms-data-api-client-javascript/main/src/dist/bundle.js"</code><br> 
to download bundle.js to your system</li>
<li>Copy bundle.js to your web directory if not in that directory already</li>
</o>
</p>
<pre>
<code class="language-html">` +
                escapeHtml(`<!-- Include the bundle.js file -->
<script src="./bundle.js"></script>
<!-- Call the cwmsjs after the bundle has loaded -->
<script type="module">
${formattedBlock.replaceAll("new ", "new cwmsjs.")}\n</script>`) +
                `</code>
</pre>`
            );
          // Write to file
          const outputFilePath = path.join(
            outputDirectory,
            `${docName.replaceAll("-", "")}.html`
          );
          fs.writeFile(outputFilePath, filledTemplate, (err) => {
            if (err) throw err;
            console.log(`Example file created: ${outputFilePath}`);
          });
        });
      } else {
        console.log(`\tNo Source Code found for ${path.basename(filePath)}`);
      }
    });
  });
  const exampleLinks =
    "<ul>" +
    all_files
      .map((filePath) => {
        const docName = path
          .basename(filePath)
          .replace(".test.js", "")
          .replaceAll("-", " ");
        return `<li><a href="/cwms-data-api-client-javascript/examples/${docName.replaceAll(" ", "")}.html">${docName.replace(".v", " - Version ")}</a></li>`;
      })
      .join("") +
    "</ul>";

  // Write the index file for all
  const indexOutputPath = path.join(outputDirectory, `index.html`);
  const filledTemplate = template
    // .replaceAll("Examples", "")
    .replaceAll("Example:", "")
    // .replaceAll(":", "")
    .replaceAll("${docName}", "Home")
    .replaceAll("${packageVersion}", packageVersion)
    .replaceAll("    ", "")
    .replaceAll(
      "${pageBody}",
      `
            ${exampleLinks}
        `
    );
  fs.writeFile(indexOutputPath, filledTemplate, (err) => {
    if (err) throw err;
    console.log(`Example file created: ${indexOutputPath}`);
  });

  // Update the modules file and index file to have links to the examples
  const indexPath = "./cwmsjs/docs/index.html";
  const modulesPath = "./cwmsjs/docs/modules.html";
  if (fs.existsSync(modulesPath)) {
    fs.readFile(modulesPath, "utf8", (err, content) => {
      if (err) throw err;
      if (content.indexOf("Examples") >= 0) return;
      const updatedContent = content.replace(
        "</h3>",
        `</h3>
             <h3 class="tsd-index-heading"><a href="/cwms-data-api-client-javascript/examples/">Examples Home</a></h3>
             ${exampleLinks}
            `
      );
      fs.writeFile(modulesPath, updatedContent, (err) => {
        if (err) throw err;
        console.log(`Updated modules file: ${modulesPath}`);
      });
    });
  }
  if (fs.existsSync(indexPath)) {
    fs.readFile(indexPath, "utf8", (err, content) => {
      if (err) throw err;
      if (content.indexOf("Examples") >= 0) return;
      const updatedContent = content.replace(
        "</h2>",
        `</h2>
             <p><a href="/cwms-data-api-client-javascript/examples/">Examples Home</a></p>
             ${exampleLinks}
            `
      );
      fs.writeFile(indexPath, updatedContent, (err) => {
        if (err) throw err;
        console.log(`Updated modules file: ${indexPath}`);
      });
    });
  }
});

function escapeHtml(unsafe) {
  return unsafe
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}
