import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduce_Query7 {

  //create main function and construct a configuration for hadoop and import all the required packages
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("DATE1", args[2]);
    conf.set("DATE2", args[3]);
    Job job = Job.getInstance(conf, "mapred sql output");
    job.setJarByClass(MapReduce_Query7.class);
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

    private Text price = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // parse the data
      String line = value.toString();
      String[] list_of_tokens = line.split("\\|");

      cust_key.set(list_of_tokens[1]);

      SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
      String date1 = context.getConfiguration().get("DATE1");
      String date2 = context.getConfiguration().get("DATE2");

      //check if the date is between the two dates
      try {
        if ((sdformat.parse(list_of_tokens[4]).after(sdformat.parse(date1)) && sdformat.parse(list_of_tokens[4]).before(sdformat.parse(date2))) || sdformat.parse(list_of_tokens[4]).equals(sdformat.parse(date1))) {
          price.set(new Text(list_of_tokens[3]));
          context.write(cust_key, price);
        }
      } catch (ParseException e) {
        e.printStackTrace();
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
