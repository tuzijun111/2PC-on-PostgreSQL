# 2PC-on-PostgreSQL

This is an Maven project, the dependencies are in the pom.xml file. The project contains two classes: Sever and Client. The server class includes the functions of the Coordinator and the Sever. The Client class is taken as a agent for a specific database.

# Steps to compile and run the codes.

1. Run the main function in the Sever class. This means the Coordinator starts to work.
2. Run the main function in the Client class. First, you will be asked to type the database name (e.g. node1 which is the name of a database) which you wanna connect to. Next, you will be asked to type "fail" or any other strings. If you type "fail", it will mean the agent reply abort to the Coordinator. Otherwise, it will send commit to the Coordinator.
3. If you wanna connect to multiple databases. You can rerun the Client class. Please allow parallel run in your java setting. Then you will see another control board which is the same one with the previous one. At this time, you can input the name of the second database, e.g. node2. Once you finish typing the database name, the new database will be active which means the new database will participate in the 2PC protocol.
4. After all the agents (i.e. the client class) of the databases indicate the state of their corresponding databases (i.e. finish typing "fail" or other string). The 2PC will move on for the Sever class.

You will see Committed or Aborted from the panel of Sever class.

# Note
You need to set the file path of the dataset (line 329 of Sever class), the file path of the requested transaction (line 80 and 327 of Sever class),, and the file path of the transaction log (line 81 of Sever class),.

Also, you can change the sensor name in line 79 of the Sever class.
