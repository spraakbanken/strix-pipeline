## Installation

Strix uses virtuals envs for python version and virtual environment management. To get running:

1. Install python and venv

    Options: 
    - You can install Python 3.x using your OS.
    - For Ubuntu: `apt-get install python3.4-venv`
2. Create the virtual environment using `python3 -m venv virtual_env`. 
3. Activate the virtual environment with `source virtual_env/bin/activate`.
4. `pip install .`

## Installation of dependencies

### Deployment

Run `install.sh`

### Local development

1. `cd ..`
2. `git clone git@github.com:spraakbanken/strix-config-configurer.git`
3. `git clone git@github.com:spraakbanken/strix-settings-sb.git`
4. cd strix-pipeline
5. Update `settings_dir` in `config.yml`
6. pip install -e ../strix-config-configurer
7. Make sure the correct branch is used in the other repositories

## Configuration

Copy config.yml.example to config.yml and make your changes. This file will be picked up by 
default, but it is also possible to run script with `--config path/to/config.yml`.

Make sure that the text and settings directories are properly configured:

```
texts_dir: /home/strix/texts
settings_dir: /home/strix/settings
```

## Elasticsearch config

Download elasticsearch with required plugin and config:

1. `wget http://demo.spraakdata.gu.se/mariao/strix/elasticsearch/strix-elasticsearch_1.0.zip`
2. Unpack and run `elasticsearch_6.2.4/bin/elasticsearch`
3. Set java heap size using (optional):
   `export ES_JAVA_OPTS="-Xms8g -Xmx8g"`

