AGE: Efficient and Adaptive Graph Online Query Processing
======

### Install

1. Clone with submodule
```
cd AGE
git submodule init
git submodule update
```

2. Set environment variable
```
export AGE_ROOT = \path\to\AGE
```

3. Install dependencies
```
sudo apt-get install wget build-essential clang-format-3.9 peg libgoogle-glog-dev libgflags-dev -y
// brpc dependencies
sudo apt-get install -y libssl-dev libgflags-dev libprotobuf-dev libprotoc-dev protobuf-compiler libleveldb-dev
```

4. Build
```
sh auto-build.sh -p CMAKE_PREFIX_PATH
```

### Start AGE Query Engine

1. Load raw csv data into partitioned AGE graphdata and send graphdata to remote
```
cd ${AGE_ROOT}
./scripts/data_importer.py
    --vertex example_graph/raw_data/vertex.csv
    --edge example_graph/raw_data/edge.csv
    --config ./config/conf.ini
    --nodes ./nodes.txt
```

2. Set up your configurations and cluster infos in config/

Configuration files includes conf.ini, conf.ini.compute, conf.ini.cache that are used to master node, compute node, and cache node, respectively. The `GLOBAL` part of these three files should be consistent. Other parts (e.g. `LOCAL`, `MODEL`) can be different. Before start the server, make sure the `GRAPH_DIR` is set to the correct path. The `GRAPH_NAME` should be consistent with configuration file that is used for data loading.

Clusters infos are set with following two files:

```
worker_nodes.txt:
[compute]
IP_ADDR:TCP_PORT:IB_PORT
[cache]
IP_ADDR:TCP_PORT:IB_PORT

master_nodes.txt:
[master]
IP_ADDR:PORT
```

Note that the master ip address and port number in `master_nodes.txt` should be consistent with `config/conf.ini.*`.

3. Start distributed server by AGE_admin
```
./scripts/AGE_admin -a start -i True
```

`-i` means to build index on start, or the user can build index with client console and standard Cypher command.

4. Start client
```
./bin/client -config=./config/conf.ini
>>> match (n: person {firstName: "Hans"} )-[:knows]->(m:person) return n.lastName, m, count(m)
```

Users can use console to adjust the batch and task size with command `config BATCH_SIZE TASK_SIZE`. Other usages can be seen with command `help`.

5. Stop distributed server by AGE_admin
```
./scripts/AGE_admin -a stop
```

### Start Throughput Evaluation

1. Prepare the throughput test configuration file (i.e. `throughput_test.conf`)
2. Start the throughput test with commands

```
cd $AGE_ROOT && ./bin/AGE_throughput_test_async \
                --config=config/conf.ini \
                --test_config=throughput_test.conf \
                --trace_generator=$random_seed \
                --random_seed=$random_seed
```

### Use Predictor to adjust the configurations
```
python model/serve.py THPT_CLIENT_ID --method="sl" --buffer_prefix="age" --epoches=1 --max_steps=1 --state_dim=STATE_DIM --action_dim=ACTION_DIM --load_model --use_wl_embeddings --enhance_cluster_info --no_opsize --use_sif
```

Notes:
1. `buffer_prefix` is used to identify the file of shared memory used to communicate with the query engine. It should be the identical with the one in conf.ini.compute.
2. `state_dim` and `action_dim` defines the input and output dimension for the model.
