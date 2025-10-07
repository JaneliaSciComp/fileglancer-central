# Fileglancer Central

Central data service for [Fileglancer](https://github.com/JaneliaSciComp/fileglancer) deployments which makes it possible for Fileglancer to access Janelia services such as JIRA. 

## Development install

Clone the repo to your local environment and change directory to the new repo folder.

```bash
git clone git@github.com:JaneliaSciComp/fileglancer-central.git
cd fileglancer-central
```

Copy the config template and edit it to your liking:

```bash
cp config.yaml.template config.yaml
```

Install package in development mode:

```bash
pixi run dev-install
```

Then run the development server:

```bash
pixi run dev-launch
```
## Production install

See [fileglancer-hub](https://github.com/JaneliaSciComp/fileglancer-hub) for details on production releases.

### Optional: configure ticket system

> [!NOTE]
> Currently, tickets are handled using JIRA. Future implementations may allow for other sources of ticket management systems.

Certain actions are handled using a ticket system so that they can be completed manually, such as complex file conversions. Configure the `atlassian_url` in the `config.yaml` file. You also need a Confluence token. Under the "User" menu in the upper right corner of the Confluence UI, click "Profile" and then "Security" and then "Create and manage API tokens". Click "Create API token" and give it a name like "Fileglancer". Copy the token, then create a `.env` file in the repo root with the following content:

```
FGC_ATLASSIAN_USERNAME=your_email
FGC_ATLASSIAN_TOKEN=your_confluence_token
```

You should set the permissions on the `.env` file so that only the owner can read it:
```
chmod 600 .env
```

If necessary, you can customize the JIRA path used for ticket links by overriding `jira_browse_url`.

## Architecture

The Fileglancer Central service is a backend service optionally used by Fileglancer to access various other services, including a shared metadata database. The diagram below shows how it fits into the larger Fileglancer deployment at Janelia. 

![Fileglancer Architecture drawio](https://github.com/user-attachments/assets/216353d2-082d-4292-a2eb-b72004087110)


## Running unit tests

```bash
pixi run test
```

## Release

First, increment the version in `pyproject.toml` and push it to GitHub. Create a *Release* there and then publish it to PyPI as follows.

To create a Python source package (`.tar.gz`) and the binary package (`.whl`) in the `dist/` directory, do:

```bash
pixi run pypi-build
```

To upload the package to the PyPI, you'll need one of the project owners to add you as a collaborator. After setting up your access token, do:

```bash
pixi run pypi-upload
```

The new version should now be [available on PyPI](https://pypi.org/project/fileglancer-central/).
