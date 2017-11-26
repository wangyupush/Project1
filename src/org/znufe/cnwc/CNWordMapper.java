package org.znufe.cnwc;

import java.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import net.paoding.analysis.analyzer.PaodingAnalyzer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;


public class CNWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(LongWritable ikey, Text ivalue, Context context)
            throws IOException, InterruptedException {
        
        byte[] bt = ivalue.getBytes();
        InputStream ip = new ByteArrayInputStream(bt);
        Reader read = new InputStreamReader(ip);
        Analyzer analyzer = new PaodingAnalyzer(); //添加庖丁分词
       
        TokenStream tokenStream = analyzer.tokenStream(word.toString(), read);

        Token t;
        while ((t = tokenStream.next()) != null)
        {
            word.set(t.termText());
            context.write(word, one);
        }
    }        
}