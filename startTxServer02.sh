#!/usr/bin/bash
cd ./02/
java -cp ../TxTest-X1.0.jar -Xms800M -Xmx800M -XX:+AlwaysPreTouch -Xlog:gc,safepoint:gc.log::filecount=0 -Dlog4j.configurationFile=../tx
server-log4j2.xml  com.hoddmimes.txtest.server.TxServer -id 2 -config ../TxServer.json
