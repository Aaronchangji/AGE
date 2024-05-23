if [ "$#" -ne 1 ]; then
    echo "Usage: $0 [TARGET_MACHINE]"
    exit -1
fi

ssh w$1 "ps aux | grep AGE_throughput_test_async | grep -v grep | \
         grep -v bash | awk '{print \$2}' | xargs -r kill -9 > /dev/null 2>&1"