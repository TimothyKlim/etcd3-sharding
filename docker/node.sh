#!/bin/bash

export NODE_ID=$(docker inspect $(hostname)|grep -o 'node_[[:digit:]]*'|grep -o "[[:digit:]]*")

/app/bin/app node
