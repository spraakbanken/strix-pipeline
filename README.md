Installation
============

Strix uses pyenv and pyvenv for python version and virtual environment management. To get running:

1a. Install [pyenv](https://github.com/yyuu/pyenv). Don't forget to add `eval "$(pyenv init -)"` to your bash profile and source it. 
1b. You can also just install the required python version using your OS. Check the `.python-version` to find which version to install. 
2. Create the virtual environment using `python3 -m venv virtual_env`. 
   With Ubuntu, first install venv: `apt-get install python3.4-venv`
3. Activate the virtual environment with `source virtual_env/bin/activate`.
4. ?????????
5. Profit

Installation
===========
1. Activate virtual env
2. Run `pip install -e .`

Tests
1. Start elasticsearch and insert suitable data (vivill)
2. Run `python setup.py test`

config.py
========
Copy strix/config.py.example to strix/config.py and set up which elastic-instance to use and data directory

Fixing permissions on stems.txt (not used right now)
===========
To be able to create an index using the custom stemmer with rules found in resources/analyzers/stems.txt:
Add the following lines (or create file) to $HOME/.java.policy in the $HOME for user running Elasticsearch, then restart
Elasticsearch:

grant {
    permission java.io.FilePermission "<path_to_your_strix_repository>/resources/analyzers/stems.txt", "read";
};

This needs to be done for every node in the cluster.

elasticsearch config
===========
1. Download and extract Elasticsearch 5.0.1
2. Install plugin at ../elasticsearch-plugin/strix-elasticsearch-plugin-1.0.zip using
   ./bin/elasticsearch-plugin install file:///<absolute path to plugin zip>
3. Set java heap size using:
   export ES_JAVA_OPTS="-Xms8g -Xmx8g"

config file
=========== 
See config.yml.example in root folder. Create your own file in the same place called "config.yml" or use 
command-line parameter "--config <file>" if file is located somewhere else.
