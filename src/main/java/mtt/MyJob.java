package mtt;

import java.util.Arrays;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.mapreduce.MultiTableInputFormat;
import org.apache.accumulo.core.data.TableKey;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;

public class MyJob extends Configured implements Tool {

  public int run(String[] arg0) throws Exception {
    System.out.println(Arrays.toString(arg0));
    Job j = new Job(getConf());
    Configuration c = j.getConfiguration();
    
    j.setJarByClass(MyJob.class);
    
    j.setInputFormatClass(MultiTableInputFormat.class);
    MultiTableInputFormat.setZooKeeperInstance(c, "ActionHank", "localhost");
    MultiTableInputFormat.setConnectionInfo(c, "root", "beards".getBytes(), Constants.NO_AUTHS);
    MultiTableInputFormat.setTables(c, "t1,t2");
    MultiTableInputFormat.setLogLevel(c, Level.TRACE);
    
    j.setMapperClass(Mapper.class);
    j.setMapOutputKeyClass(TableKey.class);
    j.setMapOutputValueClass(Value.class);
    
    j.setNumReduceTasks(0);
//    j.setReducerClass(Reducer.class);
    j.setOutputKeyClass(TableKey.class);
    j.setOutputValueClass(Value.class);
    
    j.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(j, new Path("/" + System.currentTimeMillis()));
    
    j.submit();
    j.waitForCompletion(true);
    
    return 0;
  }

  public static void main(String[] args) {
    try {
      ToolRunner.run(new MyJob(), args);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
