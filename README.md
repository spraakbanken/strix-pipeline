## Installation

We recommend using virtualenv.

## Installation of dependencies

1. `uv sync` or `pip install -e .`
2. `cd ..`
3. Use settings from server or clone this (outdated) repo: `git clone git@github.com:spraakbanken/strix-settings-sb.git`
4. `cd strix-settings-sb`
5. `git checkout dev`
6. `cd strix-pipeline`
7. Update `settings_dir` in `config.yaml`

## Configuration

Copy config.yaml.example to config.yaml and make your changes. This file will be picked up by
default, but it is also possible to run script with `--config path/to/config.yaml`.

Make sure that the text and settings directories are properly configured:

```yaml
texts_dir: /home/strix/texts
settings_dir: /home/strix/settings
```

## Elasticsearch config

Clone and build plugin, add to ES plugin folder before starting ES.

https://github.com/spraakbanken/strix-elasticsearch
