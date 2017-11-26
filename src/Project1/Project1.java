package Project1;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import org.apache.hadoop.util.*;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import org.apache.hadoop.fs.FileSystem;

@SuppressWarnings("deprecation")
public class Project1 extends Configured implements Tool {
  
   // Map类
  public static class Map extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
	  private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();
        
      @SuppressWarnings("unchecked")
	public void map(Object key, Text value, @SuppressWarnings("rawtypes") Context context
                      ) throws IOException, InterruptedException {
          byte[] bt = value.getBytes();
          InputStream ip = new ByteArrayInputStream(bt);
          Reader read = new InputStreamReader(ip);
          IKSegmenter iks = new IKSegmenter(read,true);
          Lexeme t;
          while ((t = iks.next()) != null)
          {   // 去除停用词
                  word.set(t.getLexemeText());
                  context.write(word, one);
          }
      }

	@Override
	public void map(Object arg0, Text arg1, OutputCollector<Text, IntWritable> arg2, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		
	}
  }
 
  // Reduce类
  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
      private int k;
           /**
         * 覆盖configure方法
         * */
        public void configure(JobConf job) {
          k = job.getInt("k", 0);  
        }
       
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      if(sum > k)
          output.collect(key, new IntWritable(sum));
    }
  }
 
  private static class IntWritableDecreasingComparator extends IntWritable.Comparator { 
          public int compare(@SuppressWarnings("rawtypes") WritableComparable a, @SuppressWarnings("rawtypes") WritableComparable b) { 
                    return -super.compare(a, b); 
          }
         
      public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) { 
       return -super.compare(b1, s1, l1, b2, s2, l2); 
       } 
  }
 
  // run方法
  public int run(String[] args) throws Exception {
    // 新建一个任务，用来完成对文集中单词的词频统计、词频阈值过滤和停词处理
	//Configuration conf = new Configuration();
    JobConf conf = new JobConf(getConf(), Project1.class);
    conf.setJobName("word count");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);
    conf.setInputFormat(TextInputFormat.class);
    List<String> other_args = new ArrayList<String>();
    for (int i=0; i < args.length; ++i) {
      if ("-skip".equals(args[i])) {
    	//conf.addCacheFile(new Path(args[++i]).toUri());
        DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
        conf.setBoolean("wordcount.skip.patterns", true);
      } else {
        other_args.add(args[i]);
      }
    }
    FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));;
    Path tempDir = new Path("WordCount_temp" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));             //定义一个临时目录
    FileOutputFormat.setOutputPath(conf, tempDir);                            //先将词频统计任务的输出结果写到临时目录中, 下一个排序任务以临时目录为输入目录。 
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    JobClient.runJob(conf);
   
    // 排序任务：根据每个单词出现的词频从高到低进行排序
    JobConf sortJob = new JobConf(getConf(), Project1.class);
    sortJob.setJobName("sort");
    sortJob.setJarByClass(Project1.class);
    FileInputFormat.setInputPaths(sortJob, tempDir);
    sortJob.setInputFormat(SequenceFileInputFormat.class);
    /*InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换*/ 
    sortJob.setMapperClass(InverseMapper.class);
    /*将 Reducer 的个数限定为1, 最终输出的结果文件就是一个。*/
    sortJob.setNumReduceTasks(1);
    FileOutputFormat.setOutputPath(sortJob, new Path(other_args.get(1)));
    sortJob.setOutputFormat(TextOutputFormat.class);
    sortJob.setOutputKeyClass(IntWritable.class); 
    sortJob.setOutputValueClass(Text.class);
    /*Hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排列。
     * 因此我们实现了一个 IntWritableDecreasingComparator 类,　
     * 并指定使用这个自定义的 Comparator 类对输出结果中的 key (词频)进行排序*/ 
    sortJob.setOutputKeyComparatorClass(IntWritableDecreasingComparator.class);
    JobClient.runJob(sortJob);
    FileSystem.get(conf).delete(tempDir); //删除临时目录
    return 0; 
   
  }
 
  // main方法
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Project1(), args);
    System.exit(res);
  }
}