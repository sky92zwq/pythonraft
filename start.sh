#!/bin/bash

python3 node.py --port 5003 --id 1 --peers 'localhost:5004,localhost:5005' & 
python3 node.py --port 5004 --id 2 --peers 'localhost:5003,localhost:5005' & 
python3 node.py --port 5005 --id 3 --peers 'localhost:5003,localhost:5004' &