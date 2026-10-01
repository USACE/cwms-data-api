// Local, in-memory API fixture. This script is never imported by the application.
import process from "node:process";
import { createServer } from "vite";
import {
  createDemoUsers,
  demoOffices,
  demoProfile,
  demoRoles,
} from "../tests/user-roles/demo-data.js";

const origin = "http://127.0.0.1:18742";
const authority = `${origin}/demo-auth/realms/demo`;
process.env.VITE_CDA_API_ROOT = `${origin}/demo-api`;
let users = createDemoUsers();

function json(response, body, status = 200) {
  response.writeHead(status, {
    "Content-Type": "application/json",
    "Cache-Control": "no-store",
  });
  response.end(JSON.stringify(body));
}

const server = await createServer({
  mode: "onboarding-demo",
  server: { host: "127.0.0.1", port: 18742, strictPort: true },
  plugins: [
    {
      name: "local-onboarding-demo",
      transformIndexHtml() {
        return [
          {
            tag: "aside",
            injectTo: "body-prepend",
            attrs: {
              style:
                "padding:12px 24px;background:#fef3c7;color:#78350f;font:14px sans-serif;display:flex;gap:24px;align-items:center;flex-wrap:wrap",
            },
            children:
              'Local onboarding demo — sample users only. Changes stay in memory. <form method="post" action="/demo-api/reset"><button style="cursor:pointer;text-decoration:underline" type="submit">Reset sample users</button></form>',
          },
        ];
      },
      configureServer(vite) {
        vite.middlewares.use(async (request, response, next) => {
          const url = new URL(request.url, origin);
          const path = url.pathname;
          if (path === "/cwms-data/swagger-docs") {
            return json(response, {
              openapi: "3.0.3",
              info: { title: "Local onboarding demo", version: "1" },
              paths: {},
              components: {
                securitySchemes: {
                  OpenId: {
                    type: "openIdConnect",
                    openIdConnectUrl: `${authority}/.well-known/openid-configuration`,
                    "x-oidc-client-id": "cwms",
                  },
                },
              },
            });
          }
          if (
            path.endsWith("/.well-known/openid-configuration") &&
            path.startsWith("/demo-auth/")
          ) {
            return json(response, {
              issuer: authority,
              authorization_endpoint: `${authority}/protocol/openid-connect/auth`,
              token_endpoint: `${authority}/protocol/openid-connect/token`,
              jwks_uri: `${authority}/protocol/openid-connect/certs`,
            });
          }
          if (path === "/demo-auth/realms/demo/protocol/openid-connect/token") {
            return json(response, {
              access_token: "local-demo-token",
              refresh_token: "local-demo-refresh",
              token_type: "Bearer",
              expires_in: 3600,
            });
          }
          if (path.startsWith("/demo-auth/")) return json(response, {});
          if (!path.startsWith("/demo-api/")) return next();
          if (path === "/demo-api/reset" && request.method === "POST") {
            users = createDemoUsers();
            response.writeHead(303, { Location: "/cwms-data/user-roles" });
            return response.end();
          }
          if (path === "/demo-api/user/profile") return json(response, demoProfile);
          if (path === "/demo-api/offices") return json(response, demoOffices);
          if (path === "/demo-api/roles") return json(response, demoRoles);
          if (path === "/demo-api/users") {
            const office = url.searchParams.get("office");
            const search = url.searchParams.get("username-like");
            const matches = users.filter(
              (user) =>
                (!office || user.roles[office]?.length) &&
                (!search || new RegExp(search, "i").test(user["user-name"])),
            );
            return json(response, { users: matches, total: matches.length });
          }
          const match = path.match(/^\/demo-api\/user\/([^/]+)\/roles\/([^/]+)$/);
          if (match && ["POST", "DELETE"].includes(request.method)) {
            const user = users.find(
              (item) => item["user-name"] === decodeURIComponent(match[1]),
            );
            const office = decodeURIComponent(match[2]);
            if (!user) return json(response, { message: "Unknown sample user" }, 404);
            if (!demoProfile.roles[office])
              return json(response, { message: "Office access denied" }, 403);
            try {
              let body = "";
              for await (const chunk of request) body += chunk;
              const roles = JSON.parse(body);
              if (
                !Array.isArray(roles) ||
                roles.some((role) => !demoRoles.includes(role))
              )
                return json(response, { message: "Unknown role" }, 400);
              const current = user.roles[office] ?? [];
              user.roles[office] =
                request.method === "POST"
                  ? [...new Set([...current, ...roles])]
                  : current.filter(
                      (role) => role === "All Users" || !roles.includes(role),
                    );
              response.writeHead(204);
              return response.end();
            } catch {
              return json(response, { message: "Invalid role request" }, 400);
            }
          }
          return json(
            response,
            { message: "This endpoint is not part of the local demo" },
            404,
          );
        });
      },
    },
  ],
});
await server.listen();
server.printUrls();
console.log(`Onboarding demo: ${origin}/cwms-data/user-roles`);
