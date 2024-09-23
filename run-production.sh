
set -x
cd $(dirname $0)
source venv/bin/activate

if [ $# -eq 0 ];
then
  echo "$0: Missing argument"
  exit 1
elif [ $# -gt 2 ];
then
  echo "$0: Too many arguments $@"
  exit 1
else
  rm -f ../strix-settings-sb/config/corpora/$1.yaml
  echo "createConfig $1"
  python strixpipeline/sparvDecoder.py $1
fi

echo "Config created"

# RUN = $0

for corpus in $1
do
  echo "reindexing $corpus"
  python bin/strix-pipeline.py recreate --index $corpus
  echo "run $corpus"
  python bin/strix-pipeline.py run --index $corpus
  echo "merge $corpus"
  python bin/strix-pipeline.py merge --index $corpus
done
