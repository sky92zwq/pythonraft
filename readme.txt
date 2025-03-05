# Raft Cluster Setup using gRPC

This guide will help you compile `raft.proto` using gRPC and run multiple scripts to form a Raft cluster.

## Prerequisites

- Python 3.x
- `grpcio` and `grpcio-tools` packages
- `protobuf` package

You can install the required packages using pip:

```sh
pip install grpcio grpcio-tools protobuf
```
## compile

To compile the raft.proto file, run the following command:
```sh
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
```

## Start a three-node cluster
```
python3 node.py --port 5003 --id 1 --peers 'localhost:5004,localhost:5005'
python3 node.py --port 5004 --id 2 --peers 'localhost:5003,localhost:5005'
python3 node.py --port 5005 --id 3 --peers 'localhost:5003,localhost:5004'
```

## get infos from a node
```
 curl localhost:8005/all                                                                                                                                    
{"currentTerm":4,"votedReceived":0,"votedFor":2,"commitIndex":0,"log":[],"role":1}
 curl localhost:8004/all                                                                                                                                   
{"currentTerm":4,"votedReceived":2,"votedFor":1,"commitIndex":0,"log":[],"role":3}
 curl localhost:8003/all                                                                                                                                    
{"currentTerm":4,"votedReceived":1,"votedFor":2,"commitIndex":0,"log":[],"role":1}
```
