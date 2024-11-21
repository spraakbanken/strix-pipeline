## Installation

Strix uses virtuals envs for python version and virtual environment management. To get running:

1. Create the virtual environment using `python -m venv .venv`.  (Python 3 needed)
2. Activate the virtual environment with `source .venv/bin/activate`.

## Installation of dependencies

1. `pip install --upgrade pip` 
2. `pip install -e .`
3. `cd ..`
4. `git clone git@github.com:spraakbanken/strix-config-reader.git`
5. `git clone git@github.com:spraakbanken/strix-settings-sb.git`
6. cd strix-pipeline
7. Update `settings_dir` in `config.yaml`
8. pip install -e ../strix-config-configurer


## Configuration

Copy config.yaml.example to config.yaml and make your changes. This file will be picked up by 
default, but it is also possible to run script with `--config path/to/config.yaml`.

Make sure that the text and settings directories are properly configured:

```
texts_dir: /home/strix/texts
settings_dir: /home/strix/settings
```

## Elasticsearch config

Clone and build plugin, add to ES plugin folder before starting ES.

https://github.com/spraakbanken/strix-elasticsearch
