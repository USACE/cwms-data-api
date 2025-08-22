import {
  UsaceBox,
  BadgeButton,
  Code,
  Divider,
  H3,
  H4,
} from "@usace/groundwork";

export default function FilterExpressions() {
  return (
    <>
      <UsaceBox title="Filter Expression Language - RSQL">
        The Filtered Time Series end point allows the user to supply a Filter
        Expression to reduce the amount of data returned. This user-provided
        filter is <b>not SQL</b> but instead a super-set of the Feed Item Query
        Language {"("}
        <a href="http://tools.ietf.org/html/draft-nottingham-atompub-fiql-00">
          FIQL
        </a>
        {")"} This capability is based on
        <a href="https://github.com/jirutka/rsql-parser">rsql-parser</a>
        and inspired by
        <a href="https://www.baeldung.com/rest-api-search-language-rsql-fiql">
          this article
        </a>
        . A somewhat similar approach was used to parse the filter-expression
        into an AST and then to walk the tree and make use of jOOQ to build up a
        SQL Condition.
        <H3 className="mt-4">RSQL Syntax</H3>
        <p>
          RSQL is a query language for parametrized filtering of resources. It's
          based on FIQL (Feed Item Query Language) and provides a simple yet
          powerful syntax for filtering data.
        </p>
        <H4 className="mt-4 font-semibold">Comparison</H4>
        <p>
          A comparison expression consists of three components: a selector, an
          operator, and a value
        </p>
        <H4 className="mt-4 font-semibold">Selector</H4>
        <p>
          The selector is a reference to some field in the input data. Places
          where filter-expressions are used will specify the selectors that are
          available.
        </p>
        <p>
          For TimeSeries the selectors are: value, datetime, quality,
          data_entry_date
        </p>
        <Divider text={"Operators"} />
        <H4 className="mt-4 font-semibold">Basic Operators</H4>
        <ul className="pl-5 list-disc">
          <li>
            <Code>==</Code> Equal to
          </li>
          <li>
            <Code>!=</Code> Not equal to
          </li>
          <li>
            <Code>&gt;</Code> Greater than
          </li>
          <li>
            <Code>&gt;=</Code> Greater than or equal to
          </li>
          <li>
            <Code>&lt;</Code> Less than
          </li>
          <li>
            <Code>&lt;=</Code> Less than or equal to
          </li>
          <li>
            <Code>=in=</Code> In a list of values
          </li>
          <li>
            <Code>=out=</Code> Not in a list of values
          </li>
        </ul>
        <H4 className="mt-4 font-semibold">Logical Operators</H4>
        <ul className="pl-5 list-disc">
          <li>
            <Code>;</Code> or <Code>and</Code> Logical AND
          </li>
          <li>
            <Code>,</Code> or <Code>or</Code> Logical OR
          </li>
        </ul>
        <H4 className="mt-4 font-semibold">Special Values</H4>
        <ul className="pl-5 list-disc">
          <li>
            <Code>null</Code> - Represents a null value (e.g.,
            <Code>value!=null</Code>)
          </li>
          <li>
            String values with spaces should be enclosed in double quotes (e.g.,{" "}
            <Code>name=="Example Name"</Code>)
          </li>
          <li>
            When a timestamp selector (such as datetime or data_entry_date) is
            encountered an attempt is made to convert the corresponding value
            into a Timestamp. The same timestamp parsing methods used in other
            portions of CDA (such as parsing start/end parameters) are used in
            the filter-expression feature.
          </li>
        </ul>
      </UsaceBox>

      <UsaceBox className="mt-1" title="Filter Expression Language Examples">
        <p>
          The TimeSeries end-point can return a large amount of data. Using RSQL
          filter expressions allows you to narrow results based on specific
          criteria.
        </p>
        <Divider text={"Examples"} />
        <H4 className="mt-4 font-semibold">Basic Filtering</H4>
        <ul className="pl-5 list-disc">
          <li>
            <Code>value==5.0</Code> - Find time series points where value equal
            to 5
          </li>
          <li>
            <Code>value{">"}25</Code> - Find time series with values greater
            than 25
          </li>
          <li>
            <Code>value{">="}100</Code> - Find time series with values greater
            than or equal to 100
          </li>
          <li>
            <Code>value{">"}50</Code> - Find time series with values less than
            50
          </li>
          <li>
            <Code>value{">="}75</Code> - Find time series with values less than
            or equal to 75
          </li>
          <li>
            <Code>value!=null</Code> - Find time series with non-null values
          </li>
        </ul>

        <H4 className="mt-4 font-semibold">Combining Conditions</H4>
        <ul className="pl-5 list-disc">
          <li>
            <Code>
              data_entry_date{">="}2021-04-05T00:00:00Z;value{">"}25
            </Code>
            - Find points where value is greater than 25 AND the data_entry_date
            is on or before April 4 2021
          </li>
          <li>
            <Code>value==null or value==-901</Code> - Find points where the
            value is null OR the value is equal to negative 901
          </li>
        </ul>
      </UsaceBox>
    </>
  );
}
