# MapRed_To_SQL
## This project concerns the implementation of an SQL Query into MapReduce Job

### Query:

**SELECT** 
  o_custkey, count(o_orderkey), sum(o_totalprice)
**FROM**
  orders
**WHERE**
    o_orderdate >= date '[DATE1]'
  **AND** 
    o_orderdate < date '[DATE2]'
**GROUP BY**
  o_custkey;
  
  
**Parameters**:
* [DATE1] is a date of the form YYYY-MM-DD between 1992 and 1998
* [DATE2] is a date of the form YYYY-MM-DD between 1992 and 1998


Our implementation can be fount into 


**In order to run the Map Reduce job on Hadoop we must do the following:**

1. Build the JAR from the project through IntelliJ Build Artifact mechanism
2. Start hadoop cluster (start-all.sh or start-yarn.sh start-dfs.sh)
3. Run jps command to make sure that all the services are running (Namenode, Datanode, Secondary Namenode, Resource Manager, Node Manager)
4. run the command `hadoop jar ProjectCEI526.jar MapRed_SQL /path/to/input/directory/ /path/to/output/directory 1992 1998`
5. Track the job through the given UI



