# Create the first node
docker run --name cas1 -p 9042:9042 -e CASSANDRA_CLUSTER_NAME=MyCluster -e CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch -e CASSANDRA_DC=datacenter1 -d cassandra
docker inspect --format='{{ .NetworkSettings.IPAddress }}' cas1
# Create two more nodes
docker run --name cas2 -e CASSANDRA_SEEDS="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' cas1)" -e CASSANDRA_CLUSTER_NAME=MyCluster -e CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch -e CASSANDRA_DC=datacenter1 -d cassandra
docker run --name cas3 -e CASSANDRA_SEEDS="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' cas1)" -e CASSANDRA_CLUSTER_NAME=MyCluster -e CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch -e CASSANDRA_DC=datacenter2 -d cassandra

# Try this
#docker exec -it cas1 cqlsh -f /initdb.cql
# If it does not work
wget https://downloads.apache.org/cassandra/4.0-beta1/apache-cassandra-4.0-beta1-bin.tar.gz
tar xzvf apache-cassandra-4.0-beta1-bin.tar.gz
apache-cassandra-4.0-beta1/bin/cqlsh -f initdb.cql