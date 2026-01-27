Statement
=========

Continue to allow Comma Separated Values (CSV) and JavaScript Object Notation (JSON) as formats going forward. CWMS Data API (CDA) will officially
support JSON and CSV. Additional formats may be introduced via versioned
changes or explicit content negotiation if a documented use case arises.

Purpose / Need
==============

CSV
---

CSV, without a viewing software, is also highly viewable by humans compared to
JSON.

CSV isolates tabular datasets and minimizes structural overhead, making it
easier to inspect in spreadsheet-oriented workflows. It is easily imported
into various end-user client software (MS Excel/Sheets), and does not require
extra code on the client in order to convert from one format to another when
JSON is the only option.

if end-users wanted to easily manipulate data without first needing to parse it in JSON they would
be able to do so with CSV.

Example: Sharing a link to a specific dataset. This can be handled via a direct
CDA link that when clicked, downloads the CSV file directly.

*Relies on*: ``Content-Type: text/csv`` being set by default.

JSON
----

JSON is the default response format. Some legacy endpoints currently return XML
due to upstream constraints; these are considered transitional and should be
normalized where feasible.

It works well with downstream programming needs and offers additional metadata
beyond what CSV provides.

Expectations
============

1. Query parameter (``?format=``) overrides ``Accept`` header to support browser
   downloading via URI control.

   - If neither is present, default to JSON.

2. CSV must only include the headers and data fields. No additional metadata, text, or comments at
   the top.

   - UTF-8 encoded
   - Empty values **must** return empty fields, **not** ``null``.
     Example: ``date,4.0,,`` **not** ``date,4.0,null,null``
     This ensures smaller file sizes and compatibility with common CSV parsers.

3. If data response types must change, such as headers or key-value pairs in a
   response, the version **must** be updated to reflect this in the
   ``Accept`` header to match the practices already in place for versioning. See :doc:`Versioning policy <./0004-data-versioning.rst>` for details.


4. Error handling requirements:
    
    Proper errors must be returned when a format fails to parse (e.g. invalid JSON or a CSV file that does not contain any data or fails to render).    

   - ``400 Bad Request`` MUST be returned for malformed or invalid client input
   - ``415 Unsupported Media Type`` MUST be returned when the requested format is
     recognized but not supported for the endpoint
   - ``406 Not Acceptable`` MUST be returned when the requested media type will
     never be supported by the endpoint