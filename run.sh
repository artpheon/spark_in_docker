docker-compose -f docker-compose.yml up --build -d
echo 'Starting services...'
sleep 60
docker exec kafka /bin/bash /home/start.sh || exit 1
echo 'Kafka: created topic'
sleep 20
docker exec cassandra /bin/bash /home/start.sh || exit 1
echo 'Cassandra: created DB'
sleep 5
docker exec spark_master /bin/bash /home/start.sh || exit 1
echo 'Spark: setting up Spark Streaming...'
sleep 40
docker exec app /bin/bash /home/start.sh || exit 1
echo 'App: started streaming'
sleep 20
docker exec cassandra /bin/bash cqlsh -u cassandra -p cassandra -e "select * from data.metrics;" || exit 1
echo 'Done'
