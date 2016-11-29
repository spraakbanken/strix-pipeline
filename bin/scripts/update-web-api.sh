#!/usr/bin/env bash
ssh fkstrix@demo.spraakdata.gu.se << ENDSSH
cd strix-trunk
svn up
cat run.pid | xargs kill -9
source ./virtual_env/bin/activate
export PYTHONPATH="src:resources"
pip install -r requirements.txt
nohup python bin/lb-api.py $1 > nohup.log 2>&1&
echo \$! > run.pid
ENDSSH