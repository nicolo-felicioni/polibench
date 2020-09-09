# Delete Cluster Kubernetes
minikube delete

# Install Minikube and start a Minikube engine:
minikube start --cpus=10 --memory='10g'

# Install yaml
helm install vitess vitess/helm/vitess -f vitess/examples/helm/001_benchmark.yaml

# Setup port forward
# Wait for cluster to be ready
sleep 300
./vitess/examples/helm/pf.sh &
sleep 120

mysql -h 127.0.0.1 -P 15306 < initdb.sql