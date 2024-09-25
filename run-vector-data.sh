
set -x
cd $(dirname $0)
source venv/bin/activate

for corpus in $1
do
  echo "import vectors"
  python bin/strix-pipeline.py add_vector_data $corpus
  python bin/strix-pipeline.py merge --index $corpus
done
