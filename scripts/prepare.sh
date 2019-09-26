#!/bin/bash

wget https://dl.google.com/go/go1.13.linux-amd64.tar.gz

tar -C /usr/local -xzf go1.13.linux-amd64.tar.gz

echo "export PATH=\$PATH:/usr/local/go/bin" >> /home/vagrant/.profile

apt-get update && apt-get install -y build-essential make

