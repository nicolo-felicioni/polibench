pushd cassandra && chmod +x clean.sh && popd
pushd cassandra && clean.sh && popd
pushd mysql && chmod +x clean.sh && popd
pushd mysql && ./clean.sh && popd
pushd vitess && chmod +x clean.sh && popd
pushd vitess && ./clean.sh && popd
pushd mongodb && chmod +x clean.sh && popd
pushd mongodb && ./clean.sh && popd
pushd voltdb && chmod +x clean.sh && popd
pushd voltdb && ./clean.sh && popd

cd cassandra_with_secondary; chmod +x clean.sh; ./clean.sh; cd ../;
cd mysql; chmod +x clean.sh; ./clean.sh; cd ../;
cd mongodb; chmod +x clean.sh; ./clean.sh; cd ../;
cd vitess; chmod +x clean.sh; ./clean.sh; cd ../;
cd voltdb; chmod +x clean.sh; ./clean.sh; cd ../;
