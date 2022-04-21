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


... continue writing here ...
