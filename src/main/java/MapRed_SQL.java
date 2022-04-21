import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapRed_SQL {

  //create main function and construct a configuration for hadoop and import all the required packages
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "mapred sql output");
    job.setJarByClass(MapRed_SQL.class);
    job.setMapperClass(Map.class);
//    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class Map extends Mapper<Object, Text, Text, MapWritable> {
    private Text cust_key = new Text();
    private MapWritable result = new MapWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // parse the data
      String line = value.toString();
      String[] list_of_tokens = line.split("\\|");

      cust_key.set(list_of_tokens[1]);

      result.put(new Text("orderKey") , new Text(list_of_tokens[0]));
      result.put(new Text("total_price") , new Text(list_of_tokens[3]));

      context.write(cust_key, result);
    }
  }

  public static class Reduce extends Reducer<Text, MapWritable, Text, Text> {
    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
      MapWritable result = new MapWritable();
      int count = 0;
      float total_price = 0;
      for (MapWritable val : values) {
        count++;
        for (MapWritable.Entry<Writable, Writable> entry : val.entrySet()) {
          if (entry.getKey().toString().equals("orderKey")) {
            result.put(entry.getKey(), entry.getValue());
          } else {
            total_price += Float.parseFloat(entry.getValue().toString());
          }
        }
      }
      result.put(new Text("count"), new Text(Integer.toString(count)));
      result.put(new Text("total_price"), new Text(Float.toString(total_price)));
      String totalPrice = String.valueOf(result.get(new Text("total_price")));
      String count_order = String.valueOf(result.get(new Text("count")));

      Text output = new Text( "-> Sum of Total Price = " + totalPrice + " | Count Of Orders per Order Key =" + count_order);
      System.out.println(output);

      context.write(key, output);
    }
  }




}
