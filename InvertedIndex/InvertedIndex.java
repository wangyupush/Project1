package InvertedIndex;
import java.io.IOException;  
 
import java.util.Hashtable;  
import java.util.StringTokenizer;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.input.FileSplit;  
  
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.util.GenericOptionsParser;  
@SuppressWarnings("unused")
public class InvertedIndex {  
  
      
      
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>   
    {   private Scanner s;

	@Override  
        public void map(LongWritable key, Text value, Context context)    
                throws IOException, InterruptedException   
           
        {     
            FileSplit fileSplit = (FileSplit)context.getInputSplit();  
            String fileName = fileSplit.getPath().getName();  
            String m = value.toString();
        	s = new Scanner(m);
        	String id = s.next();// 股票代号
        	String date = s.next();
        	String time = s.next();
        	String title = s.next();
        	String url = s.next();
        	
            String word;  
            IntWritable frequence=new IntWritable();  
            int one=1;  
            @SuppressWarnings({ "unchecked", "rawtypes" })
			Hashtable<String,Integer> hashmap=new Hashtable();    //key关键字选择String而不是Text，选择Text会出错     
            StringTokenizer itr = new StringTokenizer(title);  
            for(;itr.hasMoreTokens(); )   
            {     
                  
                word=itr.nextToken();  
                if(hashmap.containsKey(word)){  
                    hashmap.put(word,hashmap.get(word)+1);    //由于Map的输入key是每一行对应的偏移量，  
                                                                              //所以只能统计每一行中相同单词的个数，  
                }else{  
                    hashmap.put(word, one);                         
                  
                }  
              
            }  
              
            for(Iterator<String> it=hashmap.keySet().iterator();it.hasNext();){  
                word=it.next();  
                frequence=new IntWritable(hashmap.get(word));  
                Text fileName_frequence = new Text(fileName+"@"+url+"@"+frequence.toString()); 
   
                context.write(new Text(word),fileName_frequence);        //以”fish  doc1@1$url“ 的格式输出  
            }  
              
        }  
    }  
  
    public static class InvertedIndexCombiner extends Reducer<Text,Text,Text,Text>{  
          
          
        protected void reduce(Text key,Iterable<Text> values,Context context)  
                        throws IOException ,InterruptedException{      //合并mapper函数的输出  
           
            String fileName = null;  
            int sum=0;  
            String num;  
            String s;  
            String url = null;
            for (Text val : values) {  
                      
                    s= val.toString();  
                    String[] array = s.split("@");
                    //fileName=s.substring(0, val.find("$"));  
                    fileName=array[0];
                    url = array[1];
                    num = array[2];
                    //num = s.split("#");
                    //String nums=num.toString();
                    //url = s.substring(val.find("$")+1,val.find("@"));
                    //num=s.substring(val.find("@")+1, val.find("#")-1);      //提取“doc1@1”中‘@’后面的词频  
                    sum+=Integer.parseInt(num);  
            }  
        IntWritable frequence=new IntWritable(sum);  
        context.write(key,new Text(fileName+"@"+url+"@"+frequence.toString()));  
        }  
    }  
      
      
      
      
  
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>   
    {   @Override  
        protected void reduce(Text key, Iterable<Text> values, Context context)  
                throws IOException, InterruptedException   
        {   Iterator<Text> it = values.iterator();  
            StringBuilder all = new StringBuilder();  
            if(it.hasNext())  all.append(it.next().toString());  
            for(;it.hasNext();) {  
                all.append(";");  
                all.append(it.next().toString());                     
            }  
            context.write(key, new Text(all.toString()));  
        } 
    }  
  
    public static void main(String[] args)   
    {  
        if(args.length!=2){  
            System.err.println("Usage: InvertedIndex <in> <out>");  
            System.exit(2);  
        }  
          
      try {  
                Configuration conf = new Configuration();  
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
                  
                Job job = Job.getInstance(conf, "invertedindex");  
                job.setJarByClass(InvertedIndex.class);  
                job.setMapperClass(InvertedIndexMapper.class);  
                job.setCombinerClass(InvertedIndexCombiner.class);  
                job.setReducerClass(InvertedIndexReducer.class);  
                  
                job.setOutputKeyClass(Text.class);  
                job.setOutputValueClass(Text.class);  
                  
                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
                  
                System.exit(job.waitForCompletion(true) ? 0 : 1);  
       
        } catch (Exception e) {   
            e.printStackTrace();  
        }  
    }  
  
  
}  