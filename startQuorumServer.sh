#!/usr/bin/bash
java -cp TxTest-X1.0.jar -Dlog4j.configurationFile=./quorum-log4j2.xml  com.hoddmimes.txtest.quorum.QuorumServer  -config TxServer.json

