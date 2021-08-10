<h1 align=center>
  <img alt="Podcatcher" src="https://user-images.githubusercontent.com/14793624/126025087-08fae6dd-e9d3-4eed-9f3a-aa15661553e3.png" width="620px" />
</h1>
<br/>
<h3 align="center">
  <p>Audio media crawler for lbry.</p>
  <br/>
  <img alt="PyPI" src="https://img.shields.io/pypi/v/merge?style=for-the-badge">

  <img alt="Discord" src="https://img.shields.io/discord/557272918854336513?style=for-the-badge&logo=discord&logoColor=white">
</h3>


## Requeriments

- Python [3.8](https://www.python.org/)
- Poetry [1.1.7](https://python-poetry.org/)
- Elasticsearch [7.14.0](https://www.elastic.co/downloads/elasticsearch)


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

### Cache sync
Skip scan and sync existent cache data to elasticsearch.

```shell
poetry run podcatcher cache-sync
```

### Clear cache
Remove all files on the cache directory.
```shell
poetry run podcatcher clear-cache
```

### Drop
Remove all indices from elasticsearch and all files from the cache directory.

```shell
poetry run podcatcher drop
```
