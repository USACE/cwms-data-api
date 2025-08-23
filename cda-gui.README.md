# CDA - Graphical User Interface

## Index

- [Quick Start](#quick-start)
- [New Page](#adding-a-new-page)
- [Components](#using-pre-defined-components)

## Running the Development Server

### Standalone

You can run the CDA GUI on your local system without tomcat.

#### Perquisites

- Install [`NodeJS`](https://nodejs.org/en/download)

#### Quick Start

1. `cd gda-gui`
2. `npm install` (NodeJS Required)
3. `npm run dev`
4. Open https://localhost:5173/cwms-data

Changes will now be hot reloaded in the browser as you update your React files.

### Deployed Locally in the WAR

- 🚧 WIP

## Helpful Tips

### Adding a New Page

1.  Add a new directory to `cda-gui/src/my-page/`
2.  Create an `index.jsx` in the new directory
3.  Create a component function along the lines of:

    ```jsx
    export default function MyPage() {
      return <div>Example Page</div>;
    }
    ```

    You can view current page examples [here - /cda-guid/src/pages](/cda-gui/src/pages/)

4.  Add the new path/component to the client-side routing:

    1. Open [main.jsx](/cda-gui/src/main.jsx) in [/cda-gui/src](/cda-gui/src)
    2. Import your new component (top of file) with:
       ```jsx
       import MyPage from "./pages/my-page";
       ```
    3. Add a new Object to the `children` of `createBrowserRouter`

       ```javascript
       // Make sure to import MyPage at the top!
       { path: "my-page", element: <MyPage /> },
       ```

5.  Update [cda-gui/src/links/header-links.js](/cda-gui/src/links/header-links.js) to include your new route.  
    The common structure of a given url is:
    ```javascript
        {
            id: "tools",
            text: "Tools",
            href: "tools",
            children: {
                id: "my-page",
                text: "My Page",
                href: "/my-page"
            }
        }
    ```
    The `children` key is optional, only if you want an additional dropdown from the menu.  
    _(Groundwork supports nesting 2 deep!)_

### Using Pre-Defined Components

CWMS Data API GUI makes use of the [Groundwork](https://usace.github.io/#/), a React library to add stylized USACE themed components.

For Water Management components it uses [Groundwork-Water](https://usace-watermanagement.github.io/groundwork-water/#/), a React component library that uses [CWMSjs](https://hydrologicengineeringcenter.github.io/cwms-data-api-client-javascript/) to wrap CDA calls.

### Environment Files

CDA has 4 primary environments. Each of these environments targets what the GUI will use when deployed.

The environments each have their own file:

| Environment | File Name                                     | Host   |
| ----------- | --------------------------------------------- | ------ |
| Local       | .env.local                                    | System |
| Development | [.env.development](/cda-gui/.env.development) | Cloud  |
| Test        | [.env.test](/cda-gui/.env.test)               | Cloud  |
| Production  | [.env.production](/cda-gui/.env.production)   | Cloud  |

Where the `.env.local` file is not typically committed!
