package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.ACCEPT;
import static cwms.cda.api.Controllers.BOUNDING_OFFICE_LIKE;
import static cwms.cda.api.Controllers.CURSOR;
import static cwms.cda.api.Controllers.EXCLUDE_EMPTY;
import static cwms.cda.api.Controllers.FILTER_BASE_LOCATIONS;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.INCLUDE_ALIASES;
import static cwms.cda.api.Controllers.INCLUDE_EXTENTS;
import static cwms.cda.api.Controllers.INCLUDE_VERSIONS;
import static cwms.cda.api.Controllers.LIKE;
import static cwms.cda.api.Controllers.LOCATIONS;
import static cwms.cda.api.Controllers.LOCATION_CATEGORY_LIKE;
import static cwms.cda.api.Controllers.LOCATION_GROUP_LIKE;
import static cwms.cda.api.Controllers.LOCATION_KIND_LIKE;
import static cwms.cda.api.Controllers.LOCATION_TYPE_LIKE;
import static cwms.cda.api.Controllers.NEGATE_LOCATION_KIND_LIKE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SEARCH_TEXT;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.TIMESERIES;
import static cwms.cda.api.Controllers.TIMESERIES_CATEGORY_LIKE;
import static cwms.cda.api.Controllers.TIMESERIES_GROUP_LIKE;
import static cwms.cda.api.Controllers.UNIT_SYSTEM;
import static cwms.cda.api.Controllers.queryParamAsClass;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.UnsupportedParametersException;
import cwms.cda.data.dao.CatalogRequestParameters;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.LocationsDao;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.TimeSeriesDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dto.Catalog;
import cwms.cda.data.dto.Office;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.owasp.html.PolicyFactory;

public class CatalogController implements CrudHandler {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    private static final String TAG = "Catalog";
    public static final boolean INCLUDE_EXTENTS_DEFAULT = true;
    public static final boolean INCLUDE_VERSIONS_DEFAULT = true;
    public static final boolean EXCLUDE_EMPTY_DEFAULT = true;
    private static final int MAX_SEARCH_TEXT_LENGTH = 128;
    private static final int MAX_SEARCH_TEXT_TOKENS = 10;
    private static final Set<String> SEARCH_TEXT_OPERATORS = Set.of("AND",
        "OR",
        "NOT",
        "ABOUT",
        "EQUIV",
        "MINUS",
        "NEAR",
        "WITHIN",
        "HASPATH",
        "INPATH",
        "FUZZY",
        "STEM",
        "SOUNDEX");
    private static final Pattern TOKENIZE_PATTERN = Pattern.compile("\"[^\"]+\"|\\S+");
    private static final Pattern NORMALIZE_PATTERN = Pattern.compile("^[()]+|[()]+$");
    private static final Pattern ALNUM_PATTERN = Pattern.compile(".*\\p{Alnum}.*");

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    private static final int DEFAULT_PAGE_SIZE = 500;

    public CatalogController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(tags = {TAG}, ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(tags = {"Catalog"}, ignore = true)
    @Override
    public void delete(Context ctx, @NotNull String entry) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(tags = {"Catalog"}, ignore = true)
    @Override
    public void getAll(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = PAGE,
                description = "This end point can return a lot of data, this "
                    + "identifies where in the request you are."
            ),
            @OpenApiParam(name = CURSOR, deprecated = true,
                description = "This end point can return a lot of data, this "
                    + "identifies where in the request you are. This is an opaque"
                    + " value, and can be obtained from the 'next-page' value in "
                    + "the response. Deprecated, use " + PAGE + " instead."),
            @OpenApiParam(name = PAGE_SIZE,
                type = Integer.class,
                description = "How many entries per page returned. Default 500."
            ),
            @OpenApiParam(name = UNIT_SYSTEM,
                type = UnitSystem.class,
                description = UnitSystem.DESCRIPTION
            ),
            @OpenApiParam(name = OFFICE,
                description = "3-4 letter office name representing the district you "
                    + "want to isolate data to."
            ),
            @OpenApiParam(name = LIKE,
                description = "Posix <a href=\"regexp.html\">regular expression</a> "
                    + "matching against the id"
            ),
            @OpenApiParam(name = TIMESERIES_CATEGORY_LIKE,
                description = "Posix <a href=\"regexp.html\">regular expression</a> "
                    + "matching against the timeseries category id. Note: This parameter is "
                    + "unsupported when dataset is Locations."
            ),
            @OpenApiParam(name = TIMESERIES_GROUP_LIKE,
                description = "Posix <a href=\"regexp.html\">regular expression</a> "
                    + "matching against the timeseries group id. Note: This parameter is "
                    + "unsupported when dataset is Locations."
            ),
            @OpenApiParam(name = LOCATION_CATEGORY_LIKE,
                description = "Posix <a href=\"regexp.html\">regular expression</a> "
                    + "matching against the location category id"
            ),
            @OpenApiParam(name = LOCATION_GROUP_LIKE,
                description = "Posix <a href=\"regexp.html\">regular expression</a> "
                    + "matching against the location group id"
            ),
            @OpenApiParam(name = BOUNDING_OFFICE_LIKE,
                description = "Posix <a href=\"regexp.html\">regular expression</a> "
                    + "matching against the location bounding office. When this field is used "
                    + "items with no bounding office set will not be present in results."),
            @OpenApiParam(name = INCLUDE_EXTENTS, type = Boolean.class,
                description = "Whether the returned catalog entries should include timeseries "
                    + "extents. Only valid for TIMESERIES. Note: This parameter is "
                    + "unsupported when dataset is Locations."
                    + "Default is " + INCLUDE_EXTENTS_DEFAULT + "."),
            @OpenApiParam(name = INCLUDE_VERSIONS, type = Boolean.class,
                description = "Whether the returned catalog entries should include timeseries "
                    + "versions in the extents block. "
                    + "Only used when include-extents is enabled, otherwise it is ignored. "
                    + "Only valid for TIMESERIES. Note: This parameter is "
                    + "unsupported when dataset is Locations."
                    + "Default is " + INCLUDE_VERSIONS_DEFAULT + "."),
            @OpenApiParam(name = EXCLUDE_EMPTY, type = Boolean.class,
                description = "Specifies "
                    + "whether Timeseries that have empty extents "
                    + "should be excluded from the results.  For purposes of this parameter "
                    + "'empty' is defined as VERSION_TIME, EARLIEST_TIME, LATEST_TIME "
                    + "and LAST_UPDATE all being null. This parameter does not control "
                    + "whether the extents are returned to the user, only whether matching "
                    + "timeseries are excluded. Only valid for TIMESERIES. Note: This parameter is "
                    + "unsupported when dataset is Locations."
                    + "Default is " + EXCLUDE_EMPTY_DEFAULT + "."),
            @OpenApiParam(name = LOCATION_KIND_LIKE,
                description = "Posix <a href=\"regexp.html\">regular expression</a> matching "
                    + "against the location kind.  The location-kind is typically unset "
                    + "or one of the following: {\"SITE\", \"EMBANKMENT\", \"OVERFLOW\", "
                    + "\"TURBINE\", \"STREAM\", \"PROJECT\", \"STREAMGAGE\", \"BASIN\", "
                    + "\"OUTLET\", \"LOCK\", \"GATE\"}.  Multiple kinds can be matched "
                    + "by using Regular Expression OR clauses. For example: "
                    + "\"(SITE|STREAM)\""
            ),
            @OpenApiParam(name = FILTER_BASE_LOCATIONS, type = Boolean.class,
                description = "Specifies whether to filter the locations based on the "
                    + "base location. Default: false. If true, only sublocations "
                    + "locations will be returned. If false, all locations will be returned. "
                    + "Only supported for JSON format."),
            @OpenApiParam(name = NEGATE_LOCATION_KIND_LIKE, description = "Whether to use the location kind "
                + "regular expression to exclude locations with the specified kinds. Default is false."),
            @OpenApiParam(name = LOCATION_TYPE_LIKE,
                description = "Posix <a href=\"regexp.html\">regular expression</a> matching "
                    + "against the location type."
            ),
            @OpenApiParam(name = INCLUDE_ALIASES, type = Boolean.class,
                description = "Whether to add aliases to the catalog entries. "
                    + "Default is false. If true, the aliases will be added to the "
                    + "catalog entries in the response."),
            @OpenApiParam(name = SEARCH_TEXT,
                description = "This parameter allows the user to specify a text string to "
                    + "search locations' metadata. The search is case insensitive and is performed "
                    + "against the following fields: base location ID, sub location ID, "
                    + "combined location ID, public name, long name, description, "
                    + "map label, nearest city, location kind, and location type. "
                    + "Boolean operators are supported using AND, OR, and NOT. For example, "
                    + "'cat AND dog' requires both terms, 'cat OR dog' matches either term, "
                    + "and 'cat NOT dog' matches cat while excluding dog. Use quotes to search "
                    + "for an exact phrase, for example, '\"cat dog\"'. If multiple terms are "
                    + "provided without an operator, they are treated according to the internal "
                    + "text-search behavior and should not be assumed to mean AND. "
                    + "Search text must be no longer than " + MAX_SEARCH_TEXT_LENGTH
                    + " characters, and contain no more than " + MAX_SEARCH_TEXT_TOKENS
                    + " terms. Note: This parameter is unsupported when dataset is Timeseries."
            ),
        },
        pathParams = {
            @OpenApiParam(name = "dataset",
                type = CatalogableEndpoint.class,
                description = "A list of what data? E.g. Timeseries, Locations, Ratings, etc")
        },
        responses = {@OpenApiResponse(status = STATUS_200,
            description = "A list of elements the data set you've selected.",
            content = {
                @OpenApiContent(from = Catalog.class, type = Formats.JSONV2),
                @OpenApiContent(from = Catalog.class, type = Formats.XML)
            })
        },
        tags = {TAG}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String dataSet) {

        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = JooqDao.getDslContext(ctx);

            String valDataSet =
                ((PolicyFactory) ctx.appAttribute("PolicyFactory")).sanitize(dataSet);

            String cursor = queryParamAsClass(ctx, new String[] {PAGE, CURSOR},
                String.class, "", metrics, name(CatalogController.class.getName(), GET_ONE));

            int pageSize = queryParamAsClass(ctx, new String[] {PAGE_SIZE},
                Integer.class, DEFAULT_PAGE_SIZE, metrics,
                name(CatalogController.class.getName(), GET_ONE));

            String unitSystem = queryParamAsClass(ctx,
                new String[] {UNIT_SYSTEM,},
                String.class, UnitSystem.SI.getValue(), metrics,
                name(CatalogController.class.getName(), GET_ONE));

            String office = ctx.queryParamAsClass(OFFICE, String.class).allowNullable()
                .check(Office::validOfficeCanNull, "Invalid office provided")
                .get();

            String like = ctx.queryParamAsClass(LIKE, String.class).getOrDefault(".*");

            String tsCategoryLike = queryParamAsClass(ctx, new String[] {TIMESERIES_CATEGORY_LIKE},
                String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));

            String tsGroupLike = queryParamAsClass(ctx, new String[] {TIMESERIES_GROUP_LIKE},
                String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));

            String locCategoryLike = queryParamAsClass(ctx, new String[] {LOCATION_CATEGORY_LIKE},
                String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));

            String locGroupLike = queryParamAsClass(ctx, new String[] {LOCATION_GROUP_LIKE},
                String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));

            String boundingOfficeLike = queryParamAsClass(ctx, new String[] {BOUNDING_OFFICE_LIKE},
                String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));

            String locationKind = queryParamAsClass(ctx, new String[] {LOCATION_KIND_LIKE},
                String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));

            boolean negateLocationKind = ctx.queryParamAsClass(NEGATE_LOCATION_KIND_LIKE, Boolean.class)
                .getOrDefault(false);

            boolean filterBaseLocations = ctx.queryParamAsClass(FILTER_BASE_LOCATIONS, Boolean.class)
                .getOrDefault(false);

            String locationType = queryParamAsClass(ctx, new String[] {LOCATION_TYPE_LIKE},
                String.class, null, metrics, name(CatalogController.class.getName(), GET_ONE));
            boolean includeAliases = ctx.queryParamAsClass(INCLUDE_ALIASES, Boolean.class)
                .getOrDefault(false);
            String searchText = validateSearchText(ctx.queryParamAsClass(SEARCH_TEXT, String.class)
                .getOrDefault(null));
            String acceptHeader = ctx.header(ACCEPT);
            ContentType contentType = Formats.parseHeader(acceptHeader, Catalog.class);
            Catalog cat = null;
            if (TIMESERIES.equalsIgnoreCase(valDataSet)) {
                if (searchText != null && !searchText.isBlank()) {
                    throw new UnsupportedOperationException("Search text is not yet enabled for timeseries.");
                }
                TimeSeriesDao tsDao = new TimeSeriesDaoImpl(dsl, metrics);

                boolean includeExtents = ctx.queryParamAsClass(INCLUDE_EXTENTS, Boolean.class)
                    .getOrDefault(INCLUDE_EXTENTS_DEFAULT);
                boolean includeVersions = ctx.queryParamAsClass(INCLUDE_VERSIONS, Boolean.class)
                    .getOrDefault(INCLUDE_VERSIONS_DEFAULT);
                boolean excludeExtents = ctx.queryParamAsClass(EXCLUDE_EMPTY, Boolean.class)
                    .getOrDefault(EXCLUDE_EMPTY_DEFAULT);

                CatalogRequestParameters parameters = new CatalogRequestParameters.Builder()
                    .withOffice(office)
                    .withIdLike(like)
                    .withLocCatLike(locCategoryLike)
                    .withLocGroupLike(locGroupLike)
                    .withTsCatLike(tsCategoryLike)
                    .withTsGroupLike(tsGroupLike)
                    .withBoundingOfficeLike(boundingOfficeLike)
                    .withIncludeExtents(includeExtents)
                    .withIncludeVersions(includeVersions)
                    .withExcludeEmpty(excludeExtents)
                    .withLocationKind(locationKind)
                    .withLocationType(locationType)
                    .withIncludeAliases(includeAliases)
                    .build();

                cat = tsDao.getTimeSeriesCatalog(cursor, pageSize, parameters);

            } else if (LOCATIONS.equalsIgnoreCase(valDataSet)) {

                warnAboutNotSupported(ctx, new String[] {TIMESERIES_CATEGORY_LIKE,
                    TIMESERIES_GROUP_LIKE, EXCLUDE_EMPTY, INCLUDE_EXTENTS, INCLUDE_VERSIONS});

                CatalogRequestParameters parameters = new CatalogRequestParameters.Builder()
                    .withUnitSystem(unitSystem)
                    .withOffice(office)
                    .withIdLike(like)
                    .withLocCatLike(locCategoryLike)
                    .withLocGroupLike(locGroupLike)
                    .withBoundingOfficeLike(boundingOfficeLike)
                    .withLocationKind(locationKind)
                    .withLocationType(locationType)
                    .withFilterBaseLocations(filterBaseLocations)
                    .withNegateLocationKindLike(negateLocationKind)
                    .withSearchText(searchText)
                    .withIncludeAliases(includeAliases)
                    .build();

                LocationsDao dao = new LocationsDaoImpl(dsl);
                cat = dao.getLocationCatalog(cursor, pageSize, parameters);
            }
            if (cat != null) {
                String data = Formats.format(contentType, cat);
                ctx.contentType(contentType.toString());
                ctx.status(HttpServletResponse.SC_OK);
                requestResultSize.update(data.length());

                byte[] bytes = data.getBytes();
                ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
                ctx.res.getOutputStream().write(bytes);
            } else {
                final CdaError re = new CdaError("Cannot create catalog of requested "
                    + "information");

                logger.atInfo().log("%s with url:%s", re, ctx.fullUrl());
                ctx.json(re).status(HttpCode.NOT_FOUND);
            }
        } catch (IOException ex) {
            CdaError re = new CdaError("Failed to process request to retrieve catalog");
            logger.atSevere().withCause(ex).log("Failed to process request to retrieve catalog");
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    private static List<String> tokenizeSearchText(String searchText) {
        List<String> tokens = new ArrayList<>();
        Matcher matcher = TOKENIZE_PATTERN.matcher(searchText);
        while (matcher.find()) {
            tokens.add(matcher.group());
        }
        return tokens;
    }

    private static String normalizeSearchTextToken(String token) {
        return NORMALIZE_PATTERN.matcher(token).replaceAll("").toUpperCase(Locale.ROOT);
    }

    private static void validateBalancedQuotes(String searchText) {
        long quoteCount = searchText.chars()
            .filter(ch -> ch == '"')
            .count();

        if (quoteCount % 2 != 0) {
            throw new IllegalArgumentException(SEARCH_TEXT + " must contain balanced quotes");
        }
    }

    private static void warnAboutNotSupported(@NotNull Context ctx, String[] warnAbout) {
        Set<String> notSupported = new LinkedHashSet<>();
        Collections.addAll(notSupported, warnAbout);

        Map<String, List<String>> queryParamMap = ctx.queryParamMap();
        notSupported.retainAll(queryParamMap.keySet());

        if (!notSupported.isEmpty()) {
            throw new UnsupportedParametersException(List.copyOf(notSupported));
        }
    }

    private static String validateSearchText(String searchText) {
        if (searchText == null) {
            return null;
        }

        String trimmed = searchText.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(SEARCH_TEXT + " must not be blank");
        }

        if (trimmed.length() > MAX_SEARCH_TEXT_LENGTH) {
            throw new IllegalArgumentException(SEARCH_TEXT + " must be no longer than "
                + MAX_SEARCH_TEXT_LENGTH + " characters");
        }

        if (!ALNUM_PATTERN.matcher(trimmed).matches()) {
            throw new IllegalArgumentException(SEARCH_TEXT + " must contain at least one letter or digit");
        }

        validateBalancedQuotes(trimmed);

        List<String> tokens = tokenizeSearchText(trimmed);
        long searchTermCount = tokens.stream()
            .map(CatalogController::normalizeSearchTextToken)
            .filter(token -> !token.isEmpty())
            .filter(token -> !SEARCH_TEXT_OPERATORS.contains(token))
            .count();

        if (searchTermCount > MAX_SEARCH_TEXT_TOKENS) {
            throw new IllegalArgumentException(SEARCH_TEXT + " must contain no more than "
                + MAX_SEARCH_TEXT_TOKENS + " search terms, excluding operators like AND, OR, and NOT");
        }

        return trimmed;
    }

    @OpenApi(tags = {"Catalog"}, ignore = true)
    @Override
    public void update(Context ctx, @NotNull String entry) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

}
