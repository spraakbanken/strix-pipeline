set -x
cd $(dirname $0)
source venv/bin/activate

if [ $# -eq 0 ];
then
  echo "$0: Missing argument"
  exit 1
elif [ $# -gt 1 ];
then
  echo "$0: Too many arguments $@"
  exit 1
else
  echo "delete Corpus $1"
  curl -X DELETE 'localhost:9214/'$1'_*'
  rm ../strix-settings-sb/config/corpora/$1.yaml
  echo "Corpus is deleted"
fi

