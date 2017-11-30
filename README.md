Installation
============

Strix uses virtuals envs for python version and virtual environment management. To get running:

1. Install python and venv

    Options: 
    - You can install Python 3.x using your OS.
    - For Ubuntu: `apt-get install python3.4-venv`
2. Create the virtual environment using `python3 -m venv virtual_env`. 
3. Activate the virtual environment with `source virtual_env/bin/activate`.
4. `pip install -e .`
5. `pip install -e ../strix-config-configurer/`

Configuration
=============

Copy config.yml.example to config.yml and make your changes. This file will be picked up by 
default, but it is also possible to run script with `--config path/to/config.yml`.

Elasticsearch config
====================
1. Check out https://github.com/spraakbanken/strix-elasticsearch
2. Run `create_elasticsearch_zip.sh 5.1.1`
3. Unpack and run elasticsearch_x.x.x/bin/elasticsearch
3. Set java heap size using (optional):
   export ES_JAVA_OPTS="-Xms8g -Xmx8g"

