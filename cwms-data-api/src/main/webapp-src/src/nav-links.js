export default [
    {
        id: "home",
        text: "Home",
        href: "./",
    },
    {
        id: "swagger",
        text: "API Docs",
        href: "./swagger-ui",
        children: [
            {
                id: "swagger-ui",
                text: "Swagger UI",
                href: "./swagger-ui",
            },
            {
                id: "swagger-schema",
                text: "Swagger Docs Schema",
                href: "./swagger-docs",
            },
        ],
    },
    {
        id: "help",
        text: "Help",
        href: "#",
        children: [
            {
                id: "github-wiki",
                text: "Wiki",
                href: "https://github.com/USACE/cwms-data-api/wiki",
            },
            {
                id: "github-issues",
                text: "Report Issues",
                href: "https://github.com/USACE/cwms-data-api/issues",
            },
            {
                id: "regex",
                text: "Regular Expressions",
                href: "./regexp",
            }
        ],
    },
    {
        id: "about",
        text: "About",
        href: "#",
        children: [
            {
                id: "github-devs",
                text: "Developers",
                href: "https://github.com/USACE/cwms-data-api/graphs/contributors",
            },
            {
                id: "hec",
                text: "Hydrologic Engineering Center (HEC)",
                href: "https://www.hec.usace.army.mil/",
            }
        ],
    },
    {
        id: "client-libs",
        text: "Client Libraries",
        href: "#",
        children: [
            {
                id: "cwmsjs",
                text: "JavaScript/TypeScript",
                href: "https://hydrologicengineeringcenter.github.io/cwms-data-api-client-javascript/",
            },
            {
                id: "python",
                text: "Python",
                href: "https://github.com/HydrologicEngineeringCenter/cwms-python/tree/main",
            },
            {
                id: "java",
                text: "Java",
                href: "https://github.com/hydrologicengineeringcenter/cwms-data-api-client",
            }
        ],
    }
];