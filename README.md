# Fileglancer Central

Central data service for [Fileglancer](https://github.com/JaneliaSciComp/fileglancer) deployments which makes it possible for Fileglancer to access Janelia services such as JIRA and the Janelia Confluence Wiki. 

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

You should set the permissions on the `.env` file so that only the owner can read it:
```
chmod 600 .env
```

Run the development server:

```bash
pixi run dev-launch
```

## Architecture

The Fileglancer Central service is a backend service optionally used by Fileglancer to access various other services, including a shared metadata database. The diagram below shows how it fits into the larger Fileglancer deployment at Janelia. 

![Fileglancer Architecture drawio](https://github.com/user-attachments/assets/216353d2-082d-4292-a2eb-b72004087110)


## Building the Docker container

Run the Docker build on a Linux x86 system, replacing `<version>` with your version number:

```bash
cd docker/
export VERSION=<version>
docker build . --build-arg GIT_TAG=$VERSION -t ghcr.io/janeliascicomp/fileglancer-central:$VERSION
```

## Pushing the Docker container

```bash
docker push ghcr.io/janeliascicomp/fileglancer-central:$VERSION
docker tag ghcr.io/janeliascicomp/fileglancer-central:$VERSION ghcr.io/janeliascicomp/fileglancer-central:latest
docker push ghcr.io/janeliascicomp/fileglancer-central:latest
```


