import { Configuration, ProjectsApi } from "cwmsjs";
import fetch from "node-fetch";
global.fetch = fetch;

test("Test Projects", async () => {
  const config = new Configuration({
    basePath: "https://cwms-data.usace.army.mil/cwms-data",
  });
  const projects_api = new ProjectsApi(config);

  await projects_api
    .getProjects({
      office: "SWT",
      idMask: "KEYS*",
      pageSize: 5,
    })
    .then(async (data) => {
      expect(data?.projects).toBeDefined();
      expect(Array.isArray(data?.projects)).toBe(true);

      const firstProject = data?.projects?.[0];
      if (firstProject?.location?.name) {
        const project = await projects_api.getProjectsWithName({
          office: "SWT",
          name: firstProject.location.name,
        });
        expect(project?.location?.name).toBeDefined();
      }

      console.log(`Returned ${data?.projects?.length ?? 0} projects`);
    })
    .catch(async (e) => {
      if (e.response) {
        const error_msg = await e.response.json();
        e.message = `${e.response.url}\n${e.message}\n${JSON.stringify(
          error_msg,
          null,
          2
        )}`;
        console.error(e);
        throw e;
      } else {
        throw e;
      }
    });

  await projects_api
    .getProjectsLocations({
      office: "SWT",
      projectLike: "KEYS*",
    })
    .then((data) => {
      expect(Array.isArray(data)).toBe(true);
      console.log(`Returned ${data.length} project child-location groups`);
    });
}, 30000);
