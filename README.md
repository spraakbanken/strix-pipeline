## Installation

We recommend using virtualenv.

## Installation of dependencies

1. `pip install -e .`
2. `cd ..`
3. `git clone git@github.com:spraakbanken/strix-config-reader.git`
4. `cd strix-config-reader`
5. `git checkout v2.0`
6. `cd ..`
7. `git clone git@github.com:spraakbanken/strix-settings-sb.git`
8. cd strix-pipeline
9. pip install -e ../strix-config-configurer
10. Update `settings_dir` in `config.yaml`


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
