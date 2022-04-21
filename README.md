# MapRed_To_SQL
## This project concerns the implementation of an SQL Query into MapReduce Job

### Query:

**select** 
  o_custkey, count(o_orderkey), sum(o_totalprice)
**from**
  orders
**where**
    o_orderdate >= date '[DATE1]'
  **and** 
    o_orderdate < date '[DATE2]'
**group by**
  o_custkey;
  
  
**Parameters**:
[DATE1] is a date of the form YYYY-MM-DD between 1992 and 1998
[DATE2] is a date of the form YYYY-MM-DD between 1992 and 1998


Our implementation can be fount into 
