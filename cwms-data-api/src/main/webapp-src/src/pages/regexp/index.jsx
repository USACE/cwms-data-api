import { UsaceBox, BadgeButton } from "@usace/groundwork";


export default function RegExp() {
  return (
    <>
      <UsaceBox className="mt-8" title="Regular Expressions">
        Several of the Data API endpoints will filter the results by user
        provided Regular Expressions. Regular Expressions (regex) are a powerful
        tool for matching patterns in text.
        <br />
        The wikipedia page on regex is a good place to start learning about
        them:{" "}
        <a href="https://en.wikipedia.org/wiki/Regular_expression">
          https://en.wikipedia.org/wiki/Regular_expression
        </a>
        <br />
        Various languages have different implementations of regex. Not every
        regular expression feature is supported in each implementation. Data API
        forwards regular expressions to the database, which is currently Oracle.
        <br />
        See the Oracle documentation for a precise description of the supported
        features:
        <a href="https://docs.oracle.com/en/database/oracle/oracle-database/19/adfns/regexp.html">
          https://docs.oracle.com/en/database/oracle/oracle-database/19/adfns/regexp.html
        </a>
        <br />
        <BadgeButton color="red" className="mt-2 me-1">
          NOTE :
        </BadgeButton>
        CDA uses case-insensitive regular expressions!
      </UsaceBox>
      <UsaceBox className="mt-1" title="Regular Expression Examples">
        <p>
          The location_group endpoint can return a large number of location
          assignments. The endpoint allows the user to specify a regex on the
          location_category to filter the results.
        </p>
        <b>Given the following location categories:</b>
        <ul className="pl-5 list-disc">
          <li>cat145057</li>
          <li>cat191378</li>
          <li>cat344740</li>
          <li>cat357836</li>
          <li>cat402365</li>
          <li>locCat035226</li>
          <li>locCat043814</li>
          <li>locCat091201</li>
          <li>locCat100478</li>
        </ul>
        <p>
          Providing a regex of "cat" will match all of the above categories.
          This may be surprising. The regex does not need to match the entire
          string. It only needs to match a portion of the string. The regex
          "cat" will match all of the above categories because they all contain
          the string "cat". "CAT" will also match all the categories because the
          regex is case-insensitive.
        </p>
        <br />
        ^ matches the beginning of the string.
        <br />
        $ matches the end of the string.
        <br />
        <b>If you want to match only the categories that start with "cat"</b>
        <p>
          You can use the regex <code>"^cat"</code>. The "^" character is a
          special character that matches the beginning of the string. The regex{" "}
          <code>"^cat"</code> will match the first 5 categories in the list
          above.
        </p>
        . matches any character
        <br />
        <p>
          To find categories that start with cat and contain a 6 in any position
          you could use the regex <code>"^cat.*6"</code>
        </p>
        <br />
        <b>
          Sometimes you want to match a literal period character and don't want
          it to be interpreted as a wildcard.
        </b>
        <p>
          To match a literal period character you can escape it with a
          backslash. To match the string "CWMS 3.3" you can use the regex "CWMS
          3\.3" To match the string "ALBT.Stage" you can use the regex{" "}
          <code>"ALBT\.Stage"</code> or <code>"ALBT[.]Stage"</code> or{" "}
          <code>ALBT[\.]Stage</code>
        </p>
        <br />
        <b>
          To match cat145057 <em>and</em> locCat043814
        </b>
        <p>
          you can build a regular expression with the "|" character which is the
          OR operator. The regex "^cat145057$|^locCat043814$" will match both of
          those categories and only those two categories.
        </p>
        <br />
        <b>It's possible to match ranges of characters</b>
        <p>
          The regex "cat[0-3]" will match any category that contains "cat"
          followed by a 0,1,2 or 3 This will not match cat402365 because the 4
          is not in the range 0-3.
        </p>
        <br />
      </UsaceBox>
    </>
  );
}
