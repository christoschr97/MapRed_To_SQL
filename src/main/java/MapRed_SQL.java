import java.io.IOException;
import java.util.Set;

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
    conf.set("DATE1", args[2]);
    conf.set("DATE2", args[3]);
    Job job = Job.getInstance(conf, "mapred sql output");
    job.setJarByClass(MapRed_SQL.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class Map extends Mapper<Object, Text, Text, Text> {
    private Text cust_key = new Text();
//    private MapWritable result = new MapWritable();

    private Text price = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // parse the data
      String line = value.toString();
      String[] list_of_tokens = line.split("\\|");

      cust_key.set(list_of_tokens[1]);

      //split list_of_tokens[4] into list of tokens using the char '-'
      String[] list_of_items = list_of_tokens[4].split("-");

      //parse list_of_items[0] into integer and check if is between "1992" and "1998"
      int year = Integer.parseInt(list_of_items[0]);

      Configuration conf = context.getConfiguration();
      int DATE1 = Integer.parseInt(conf.get("DATE1"));
      int DATE2 = Integer.parseInt(conf.get("DATE2"));

      if (year >= DATE1 && year < DATE2) {
        price.set(new Text(list_of_tokens[3]));
        context.write(cust_key, price);

      }
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      float total_price = 0;
      for (Text val : values) {
        count++;
        total_price += Float.parseFloat((val.toString()));
      }
      String totalPrice = String.valueOf(total_price);
      String count_order = String.valueOf(count);

      Text output = new Text( "-> Sum of Total Price = " + totalPrice + " | Count Of Orders per Order Key =" + count_order);
      System.out.println(output);

      context.write(key, output);
    }
  }
}
