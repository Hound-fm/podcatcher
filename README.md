<h1 align=center>
  <img alt="Podcatcher" src="https://user-images.githubusercontent.com/14793624/126025087-08fae6dd-e9d3-4eed-9f3a-aa15661553e3.png" width="620px" />
</h1>
<br/>
<h3 align="center">
  <p>Podcast crawler for lbry.</p>
  <br/>
  <img alt="PyPI" src="https://img.shields.io/pypi/v/merge?style=for-the-badge">
</h3>



## Development
This project uses [poetry](https://python-poetry.org/) as a dependecy management tool.

### Install dependencies:
Installs all defined dependencies of the project.
For more information please read the poetry [documentation](https://python-poetry.org/docs/basic-usage/#installing-dependencies).

```shell
poetry install
```

### Format code:
Formore information please read the black [documentation](https://github.com/psf/black)
```shell
poetry run black ./src
```

## Commands

### Basic usage

For more information please read the poetry [documentation](https://python-poetry.org/docs/basic-usage/#using-poetry-run).

```shell
poetry run podcatcher <command>
```


### Sync
Scan all audio streams to find music and podcasts episodes, keeping elasticsearch in sync.

```shell
poetry run podcatcher sync
```

### Retry sync

Retry failed sync from last checkpoint. If no previous failed sync occured it will just run a normal sync.
```shell
poetry run podcatcher retry-sync
```

### Clear cache
Remove all files on the cache directory.
```shell
poetry run podcatcher clear-cache
```
