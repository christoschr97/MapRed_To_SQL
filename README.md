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

1. Copy the project or unzip it
2. Open it with IntelliJ
3. Run `mvn clean install` to the root foldre of the project 
4. Find the Jar in the Target Folder with the name `ProjectCEI526-1.0-SNAPSHOT.jar` 
5. Copy the data to HDFS `hdfs dfs -copyFromLocal /target/data/path /destination/path`
6. Run the command `hadoop jar ProjectCEI526-1-0-SNAPSHOT.jar /input/data/path/ /output/data/path/ 1992-01-01 1998-01-01`



