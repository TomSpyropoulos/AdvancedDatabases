#!/bin/bash
start-dfs.sh
start-yarn.sh
$SPARK_HOME/sbin/start-history-server.sh
hdfs dfsadmin -safemode leave
export PYSPARK_PYTHON=./venv/bin/python
spark-submit --archives venv.tar.gz#venv req_1.py
spark-submit --archives venv.tar.gz#venv query_1.py
spark-submit --archives venv.tar.gz#venv query_1_sql.py
spark-submit --archives venv.tar.gz#venv query_2.py
spark-submit --archives venv.tar.gz#venv query_2_rdd.py
spark-submit --archives venv.tar.gz#venv query_3.py
spark-submit --archives venv.tar.gz#venv query_3_joins.py broadcast
spark-submit --archives venv.tar.gz#venv query_3_joins.py merge
spark-submit --archives venv.tar.gz#venv query_3_joins.py shuffle_hash
spark-submit --archives venv.tar.gz#venv query_3_joins.py shuffle_replicate_nl
spark-submit --archives venv.tar.gz#venv query_4a.py
spark-submit --archives venv.tar.gz#venv query_4a_joins.py broadcast
spark-submit --archives venv.tar.gz#venv query_4a_joins.py merge
spark-submit --archives venv.tar.gz#venv query_4a_joins.py shuffle_hash
spark-submit --archives venv.tar.gz#venv query_4a_joins.py shuffle_replicate_nl
spark-submit --archives venv.tar.gz#venv query_4b.py
spark-submit --archives venv.tar.gz#venv query_4b_joins.py broadcast
spark-submit --archives venv.tar.gz#venv query_4b_joins.py merge
spark-submit --archives venv.tar.gz#venv query_4b_joins.py shuffle_hash
spark-submit --archives venv.tar.gz#venv query_4b_joins.py shuffle_replicate_nl
stop-dfs.sh
stop-yarn.sh
$SPARK_HOME/sbin/stop-history-server.sh
rm venv.tar.gz
