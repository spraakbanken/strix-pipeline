Installation
============

Strix uses pyenv and pyvenv for python version and virtual environment management. To get running:

1a. Install [pyenv](https://github.com/yyuu/pyenv). Don't forget to add `eval "$(pyenv init -)"` to your bash profile and source it. 
1b. You can also just install the required python version using your OS. Check the `.python-version` to find which version to install. 
2. Create the virtual environment using `python3 -m venv virtual_env`. 
   With Ubuntu, first install venv: `apt-get install python3.4-venv`
3. Activate the virtual environment with `source virtual_env/bin/activate`.
4. Run `pip install -e .`

Tests
=====
1. Start Elasticsearch and run `setuptest.py` from `test_strix/data` (this will insert data on localhost:9200 and will overwrite any indexes
   that has the same names as the test-set)
2. Run `python setup.py test`

Elasticsearch config
====================
1. Download and extract Elasticsearch 5.0.1
2. Install plugin at ../elasticsearch-plugin/strix-elasticsearch-plugin-1.0.zip using
   ./bin/elasticsearch-plugin install file:///<absolute path to plugin zip>
3. Set java heap size using:
   export ES_JAVA_OPTS="-Xms8g -Xmx8g"
4. Copy `stems.txt` from `strix/resources/analyzers/` to Elasticsearch's `config`-directory

Config file
=========== 
See config.yml.example in root folder. Create your own file in the same place called "config.yml" or use 
command-line parameter "--config <file>" if file is located somewhere else.

Test environment
======================

fkstrix@kork:
There is an elasticsearch instance running on port 9212. The cluster is called
`strix-test` and the node name is `kork-test`.

fkstrix@koala:
How to reindex the index the test data set:
```
ssh koala
cd strix-test
svn up
source virtual_env/bin/activate
cd test_strix/data/
python ./setuptest.py
```

fkstrix@k2:
The test backend is in `/export/htdocs_sbws/ws/strixlabb/strix-test`.

How to restart the test backend:
```
cd -P test-backend
svn up
supervisorctl -c /etc/supervisord.d/fkstrix.conf restart strix-test-api
```
