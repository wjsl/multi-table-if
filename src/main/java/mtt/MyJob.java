package mtt;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.multi.AccumuloInputFormat;
import org.apache.accumulo.core.data.TableKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyJob extends Configured implements Tool {

  public int run(String[] arg0) throws Exception {
    System.out.println(Arrays.toString(arg0));
    Job j = new Job(getConf());
    Configuration c = j.getConfiguration();
    
    j.setJarByClass(MyJob.class);
    
    j.setInputFormatClass(AccumuloInputFormat.class);
    AccumuloInputFormat.setZooKeeperInstance(c, "actionhank", "localhost");
    List<String> tables = Arrays.asList("ci1", "ci2");
    AccumuloInputFormat.setInputInfo(c, "root", "beards".getBytes(), tables, Constants.NO_AUTHS);
    
    Map<String, Collection<IteratorSetting>> itrs = new HashMap<String, Collection<IteratorSetting>>();
    itrs.put("ci1", Arrays.asList(
        new IteratorSetting(25, Stub1Iterator.class) ) );
    itrs.put("ci2", Arrays.asList(
        new IteratorSetting(25, Stub2Iterator.class) ) );
    AccumuloInputFormat.setIterators(j.getConfiguration(), itrs);
    
    j.setMapperClass(Mapper.class);
    j.setMapOutputKeyClass(TableKey.class);
    j.setMapOutputValueClass(Value.class);
    
    j.setNumReduceTasks(1);
    j.setReducerClass(Reducer.class);
    j.setOutputKeyClass(TableKey.class);
    j.setOutputValueClass(Value.class);
    
    j.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(j, new Path("/out.txt"));
    
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
  
  public static class Stub1Iterator extends WrappingIterator {
    static final Value v = new Value( new Text( "stub1" ).getBytes() );
    public Value getTopValue() {
      return v;
    }
  }
  
  public static class Stub2Iterator extends WrappingIterator {
    static final Value v = new Value( new Text( "stub2" ).getBytes() );
    public Value getTopValue() {
      return v;
    }
  }
}
