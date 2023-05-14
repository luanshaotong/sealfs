loop_num=10

# create 10 dirs in ~/fs, named form test1 to test10, create a test.log in each dir
# for ((i=1; i<=loop_num; i++))
# do
#     mkdir ~/fs/test_rm$i
#     echo "test" >> ~/fs/test_rm$i/test.log
# done

target/debug/client --log-level info delete 127.0.0.1:8089

sleep 20

# for ((i=1; i<=loop_num; i++))
# do
#     cat ~/fs/test_rm$i/test.log
# done

kill $(jobs -p)