Installation
============

Strix uses pyenv and pyvenv for python version and virtual environment management. To get running:

1a. Install [pyenv](https://github.com/yyuu/pyenv). Don't forget to add `eval "$(pyenv init -)"` to your bash profile and source it. 
1b. You can also just install the required python version using your OS. Check the `.python-version` to find which version to install. 
2a. Create the virtual environment using `pyvenv virtual_env`. 
2b. With ubuntu: `apt-get install python3.4-venv` and run with `python -m venv virtual_env`
3. Activate the virtual environment with `source virtual_env/bin/activate`.
4. Profit. 

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
