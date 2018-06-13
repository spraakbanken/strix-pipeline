
rm -r virtual_env
rm -r config

if [ ! -f config.yml ]; then
    echo "elastic_hosts: [{host: XXXXXX, port: XXXX}]" > config.yml
    echo "base_dir: ." >> config.yml
    echo "settings_dir: config" >> config.yml
    echo "texts_dir: ???" >> config.yml
    echo "concurrency_upload_threads: X" >> config.yml
    echo "concurrency_queue_size: X" >> config.yml
    echo "concurrency_group_size: X" >> config.yml

    echo "number_of_shards: X" >> config.yml
    echo "number_of_replicas: 1" >> config.yml
    echo "terms_number_of_shards: X" >> config.yml
    echo "terms_number_of_replicas: 1" >> config.yml
fi


python3 -m venv virtual_env

source virtual_env/bin/activate

pip install --upgrade pip
pip install .

wget http://demo.spraakdata.gu.se/mariao/strix/settings/strix-sbconfig_1.0.dev.zip
unzip strix-sbconfig_1.0.dev.zip
rm strix-sbconfig_1.0.dev.zip
pip install http://demo.spraakdata.gu.se/mariao/strix/python/strixconfigurer-1.0.dev0.zip

