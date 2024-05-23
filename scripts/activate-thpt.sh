if [ "$#" -ne 3 ]; then
    echo "Usage: $0 [TARGET_MACHINE] [RANDOM_SEED:true|false] [AGE_ROOT]"
    exit -1
fi

random_seed="$(echo $2 | awk '{print tolower($0)}')"
if [ "$random_seed" != "true" ] && [ "$random_seed" != "false" ]; then
    echo "Usage: $0 [TARGET_MACHINE] [RANDOM_SEED:true|false] [AGE_ROOT]"
    echo "Wrong RANDOM_SEED value: $2"
    exit -1
fi

ssh -f w$1 "export AGE_ROOT=$3 && cd \$AGE_ROOT && \
            ./bin/AGE_throughput_test_async \
                --config=config/conf.ini \
                --test_config=throughput_test.conf \
                --trace_generator=$random_seed \
                --random_seed=$random_seed > /dev/null 2>&1"