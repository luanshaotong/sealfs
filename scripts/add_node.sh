loop_num=1000

# create 10 dirs in ~/fs, named form test1 to test10, create a test.log in each dir
for ((i=1; i<=loop_num; i++))
do
    mkdir ~/fs/test$i
    echo "test" >> ~/fs/test$i/test.log
done

target/debug/client --log-level info add 127.0.0.1:8090
./target/debug/server --server-address 127.0.0.1:8090 --database-path /data/database5/ --storage-path /data/storage5/ --log-level info &

sleep 20

for ((i=1; i<=loop_num; i++))
do
    cat ~/fs/test$i/test.log
done

kill $(jobs -p)