# Install Minikube and start a Minikube engine:
minikube start

# Install Helm 3:
sudo snap install helm

# Install the MySQL client locally. For example, on Ubuntu:
sudo apt install mysql-client

# Install vtctlclient locally:
go get vitess.io/vitess/go/cmd/vtctlclient

# Change to the helm example directory:
git clone https://github.com/vitessio/vitess
cd vitess/examples/helm

# Install yaml
helm install vitess ../../helm/vitess -f 001_benchmark.yaml

# Setup port forward
./pf.sh &
sleep 5
alias vtctlclient="~/go/bin/vtctlclient -server=localhost:15999"
alias mysql="mysql -h 127.0.0.1 -P 15306"

mysql < initdb.sql