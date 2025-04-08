#!/usr/bin/bash
java -cp TxTest-X1.0.jar  com.hoddmimes.txtest.client.TxClient  -host 127.0.0.1 -port 4001 -threads 6 -rate 0 -count 3000000
