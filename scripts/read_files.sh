for ((i=1; i<=100; i++))
do
    mkdir ~/fs/test_rm$i
    echo "test" >> ~/fs/test_rm$i/test.log
done


for ((i=1; i<=100; i++))
do
    for ((j=1; j<=100; j++))
    do
        cat ~/fs/test_rm$i/test.log
        sleep 0.01
    done
done

kill $(jobs -p)