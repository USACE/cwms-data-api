Development in docs module:

.rst title/section hierarchies for this project follow this convention:

1. Title ========
2. Section ---------
3. Subsection ~~~~~~
4. Subsubsection ^^^^^


\cwms-data-api\docs> .venv\Scripts\Activate.ps1
(.venv) 
\cwms-data-api\docs> pip install -r requirements.txt
\cwms-data-api\docs> .\make.bat html or .\make html


OR in Gradle Tab:
docs -> Tasks -> other -> buildDocs

Both of these options will build the docs in the docs/build/html directory.
find the index.html file in that directory, or the specific page you want, and open it in a browser.


OR use Sphinx autobuild:
\cwms-data-api\docs> sphinx-autobuild source build/html --open-browser

output:
The HTML pages are in build\html.
[sphinx-autobuild] Serving on http://127.0.0.1:8000
[sphinx-autobuild] Waiting to detect changes...

click the link in the terminal to open the docs in a browser.


Run this command to stop the venv:
Deactivate current venv: deactivate