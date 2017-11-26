package LuceneChinese;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import java.util.Scanner;
@SuppressWarnings("unused")
public class ChineseWordCount {
    
	private static boolean exit;
	
	private static String skipfile;
	private static int min_num;
	private static String tempDir;
	private static String[] otherArgs;
	public static class TokenizerMapper 
           extends Mapper<Object, Text, Text, IntWritable>{
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
		private Scanner s;
          
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
        	String m = value.toString();
        	s = new Scanner(m);
        	String id = s.next();// 股票代号
        	String name = s.next();//股票名称
        	String date = s.next();
        	String time = s.next();
        	String title = s.next();
        	String url = s.next();
        	
            byte[] bt = title.getBytes();
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
      }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    int k=Integer.parseInt(otherArgs[2]);
  //使用result记录词频。为IntWritable类型。
    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      if(sum > k) {
          //output.collect(key, new IntWritable(sum));
          context.write(key, result);
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
  public static void main(String[] args) throws Exception {
	  exit = false;  
      skipfile = null;
      min_num = 0;  
      tempDir = "wordcount-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE));  
    Configuration conf = new Configuration();
    otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
    if(otherArgs.length != 3) {
    	System.err.println("Please input : <inputPath> <outputPath> <k>");
    	System.exit(2);
    }
    
  //获取要展示的最小词频  
    for(int i=0;i<args.length;i++)  
    {  
        if("-greater".equals(args[i])){  
            min_num = Integer.parseInt(args[++i]);  
            System.out.println(args[i]);  
        }             
    }  
      
    //将最小词频值放到Configuration中共享  
    conf.set("min_num", String.valueOf(min_num));   //set global parameter 
    
    try{  
        /** 
         * run first-round to count 
         * */  
        Job job = Job.getInstance(conf, "wordcountjob-1");  
        job.setJarByClass(ChineseWordCount.class);  
          
        //set format of input-output  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(SequenceFileOutputFormat.class);  
          
        //set class of output's key-value of MAP  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);  
          
        //set mapper and reducer  
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
          
        //set path of input-output  
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
        FileOutputFormat.setOutputPath(job, new Path(tempDir));  
          
        if(job.waitForCompletion(true)){              
            /** 
             * run two-round to sort 
             * */  
            //Configuration conf2 = new Configuration();  
            Job job2 = Job.getInstance(conf, "wordcountjob-2");  
            job2.setJarByClass(ChineseWordCount.class);  
              
            //set format of input-output  
            job2.setInputFormatClass(SequenceFileInputFormat.class);  
            job2.setOutputFormatClass(TextOutputFormat.class);        
              
            //set class of output's key-value  
            job2.setOutputKeyClass(IntWritable.class);  
            job2.setOutputValueClass(Text.class);  
              
            //set mapper and reducer  
            //InverseMapper作用是实现map()之后的数据对的key和value交换  
            //将Reducer的个数限定为1, 最终输出的结果文件就是一个  
            /** 
            * 注意，这里将reduce的数目设置为1个，有很大的文章。 
            * 因为hadoop无法进行键的全局排序，只能做一个reduce内部 
            * 的本地排序。 所以我们要想有一个按照键的全局的排序。 
            * 最直接的方法就是设置reduce只有一个。 
            */  
            job2.setMapperClass(InverseMapper.class);      
            job2.setNumReduceTasks(1); //only one reducer  
              
            //set path of input-output  
            FileInputFormat.addInputPath(job2, new Path(tempDir));  
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));  
              
            /** 
             * Hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排列。 
             * 因此我们实现了一个 IntWritableDecreasingComparator 类,　 
             * 并指定使用这个自定义的 Comparator 类对输出结果中的 key (词频)进行排序 
             * */  
            job2.setSortComparatorClass(IntWritableDecreasingComparator.class);  
            exit = job2.waitForCompletion(true);  
        }  
    }catch(Exception e){  
        e.printStackTrace();  
    }finally{  
          
        try {  
            //delete tempt dir  
            FileSystem.get(conf).deleteOnExit(new Path(tempDir));  
            if(exit) System.exit(1);  
            System.exit(0);  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
  }
  }
}