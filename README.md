# Fileglancer Central

Central metadata server for [Fileglancer](https://github.com/JaneliaSciComp/fileglancer) deployments.

## Development install

Clone the repo to your local environment and change directory to the new repo folder.

```bash
git clone git@github.com:JaneliaSciComp/fileglancer-central.git
cd fileglancer-central
```

If this is your first time installing the extension in dev mode, install package in development mode.

```bash
pixi run dev-install
```

> [!NOTE]
> Currently, the file share paths are only pulled from Janelia's Confluence wiki. Future implementations will allow for other sources of file share paths.

To pull file share paths from Janelia's Confluence wiki, you need a Confluence token. Under the "User" menu in the upper right corner of the Confluence UI, click "Settings" and then "Personal Access Tokens". Click "Create token" and give it a name like "Fileglancer Central". Copy the token, then create a `.env` file in the repo root with the following content:

```
FGC_CONFLUENCE_TOKEN=your_confluence_token
```

Run the development server:

```bash
pixi run dev-launch
```

