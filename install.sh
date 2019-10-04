
strix_es_version="1.2" # es7
configurer_version="1.0"

rm -r .venv
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


python3 -m venv .venv

source .venv/bin/activate

pip install --upgrade pip
pip install .

pip install http://demo.spraakdata.gu.se/mariao/strix/python/strixconfigurer-$configurer_version.zip

echo ""
echo "---------------------------------"
echo "Make sure you have the correct version of strix elasticsearch"
echo "http://demo.spraakdata.gu.se/mariao/strix/elasticsearch/strix-elasticsearch_$strix_es_version.zip"
echo "---------------------------------"

echo ""
echo "To update settings"
echo "wget http://demo.spraakdata.gu.se/mariao/strix/settings/strix-sbconfig_$configurer_version.zip"
echo "rm -r config/"
echo "unzip strix-sbconfig_$configurer_version.zip"
echo "---------------------------------"
