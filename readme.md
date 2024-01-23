### How to run
Αφού ρυθμιστεί το περιβάλλον του hadoop, yarn και spark όπως αναφέρεται στον οδηγό, αρκεί να ανέβουν τα dataset στο φάκελο /datasets του hdfs με την χρήση της εντολής:
```
hadoop fs -put ./datasets /
```
Έπειτα εκτελούμε τις εντολές:
```
sudo apt install python3-pip
sudo apt install python3-virtualenv
./run.sh
```
Το bash script run.sh εκτελεί τα αρχεία του repository και δίνει τα σωστά command line arguments.
Μετά την χρήση του τα deamons του hadoop και yarn, αλλά και ο spark history server παραμένουν ανοιχτά οπότε θα πρέπει να κλείσουν με τις εντολές:
```
stop-dfs.sh
stop-yarn.sh
$SPARK_HOME/sbin/stop-history-server.sh
```
