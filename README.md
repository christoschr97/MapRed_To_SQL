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


Our implementation can be fount into `src/main/MapReduce_Query7.java`

## Usage Instructions
**In order to run the Map Reduce job on Hadoop we must do the following:**

1. Clone the project from Github or unzip the deliverable assigned with the Project Report
2. Open it with IntelliJ/Eclipse (or any other JDK suitable for Java Projects)
3. Run `mvn clean install` to the root foldre of the project 
4. Find the Jar in the Target Folder with the name `ProjectCEI526-1.0-SNAPSHOT.jar` 
5. Copy the data to HDFS `hdfs dfs -copyFromLocal /target/data/path /destination/path`
6. Run the command `hadoop jar ProjectCEI526-1-0-SNAPSHOT.jar /input/data/path/ /output/data/path/ 1992-01-01 1998-01-01`

The target folder is present and thus it doesn't require any installation with maven. Although, if its necessary to re-build the project the following steps must be followed:
1. Delete the target folder
2. Run mvn clean install 
3. Find the JAR as stated in the above usage instructions in the target folder of the project


,
