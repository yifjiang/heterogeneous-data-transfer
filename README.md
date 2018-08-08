# heterogeneous-data-transfer

## Prerequisite

1. JDK
2. Maven
3. Kafka

## Setup

1. `cd` into this repository directory.
2. Change the `sqlConnectionUrl` and `monitorDBURL` variable in `com/datayes/heterDataTransfer/sync/ServerConfig.java` and `com/datayes/heterDataTransfer/server/ServerConfig.java` according to the address, port, database name, user, and password of the source database.
4. Change the `tableName` variable in `com/datayes/heterDataTransfer/server/ServerConfig.java` to the name of the table to be synchronized.
3. Change the `sqlConnectionUrl` and `monitorDBURL` variable in `com/datayes/heterDataTransfer/sync/ClientConfig.java` and `com/datayes/heterDataTransfer/client/ClientConfig.java` according to the address, port, database name, user, and password of the client's database.
4. Change the `tableToSynchronize` variable in `com/datayes/heterDataTransfer/sync/ClientConfig.java` to the names of the table to be synchronized.
2. In a terminal, run: `maven package`
3. Copy `heterogeneous-data-transfer-client.jar` and `heterogeneous-data-transfer-fullClient.jar` to the client side machine.
4. Copy `heterogeneous-data-transfer-server.jar` and `heterogeneous-data-transfer-fullServer` to the server.
5. Startup Kafka.
6. To startup the server-side incremental capturing program:
        
        java -jar heterogeneous-data-transfer-server.jar

6. To startup the client side incremental capturing program:

        java -jar heterogeneous-data-transfer-client.jar

7. To run the server-side full synchronization program:
        
        java -jar heterogeneous-data-transfer-fullServer.jar

8. To run the client-side full synchronization program:
        
        java -jar heterogeneous-data-transfer-fullClient.jar