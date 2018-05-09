package com.pl.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.log.Log;

import com.pl.index.dfsConfig.h_mode;


/*
 * 实现功能 ：字符在文件里的出现次数，字符合并
=======================================
输入 
	hello-->index01.txt	3
	hello-->index02.txt	1
	hello-->index03.txt	1
	jerry-->index03.txt	1
	jerry-->index01.txt	2
	jerry-->index02.txt	2
	tom-->index01.txt	1
	tom-->index02.txt	1

 =======================================
运行结果 
hello	index03.txt-->1	index02.txt-->1	index01.txt-->3	
jerry	index03.txt-->1	index02.txt-->2	index01.txt-->2	
tom	index02.txt-->1	index01.txt-->1	

*/
public class indexMerge {
	
	public static class indexMergeMapper extends Mapper<LongWritable, Text, Text, Text>{ 
	 
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException { 
			
			//切分单词
			String lineInfo = value.toString();
			String[] words = lineInfo.split("-->"); 
			
			String word = words[0]; 
			String lineInfosub = words[1]; 
			String[] wordsubs =  lineInfosub.split("\t"); 
			String filename = wordsubs[0]; 
			String wordcount = wordsubs[1]; 
			Log.info(" word=[" + word + "] lineInfosub=[" + lineInfosub + "]");
			Log.info(" filename=[" +filename + "] wordcount=[" + wordcount+ "]");
			//System.out.println(" words[0]=" +word + " words[1]=" + word);
			//hello-->index01.txt	3
			//kv输出格式：  key=[hello]	value=[index01.txt->3]
            context.write(new Text(word), new Text(filename+"-->"+wordcount));  
		} 
	}
	
	public static class indexMergeReducer extends Reducer<Text, Text, Text, Text>{
	 
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException { 
			
			// hello [index01.txt->3,index02.txt->1,index03.txt->1]
			String line = "";
			for	(Text v:values) {
				line +=v+ "\t";  
			}
			context.write(key, new Text(line)); 
		}
	}
		
	static String dfs_path_in = dfsConfig.DFS_PATH_OUT + "/index";
	static String dfs_path_out = dfsConfig.DFS_PATH_OUT + "/merge";
	static String dfs_full_path_in = dfsConfig.DFS_FULL_PATH_OUT + "/index";
	static String dfs_full_path_out = dfsConfig.DFS_FULL_PATH_OUT + "/merge";
	
	public static void main(String[] args) throws Exception {
		
		//配置服务器 
        Configuration conf = dfsConfig.getConf(h_mode.HA);
        //删除历史输出目录
        FileSystem fileSystem = FileSystem.get(conf); 
    	boolean reuslt= fileSystem.exists(new Path(dfs_full_path_out));
    	if (reuslt) {
	    	reuslt = fileSystem.delete(new Path(dfs_full_path_out), true);
			System.out.println(dfsConfig.LOG_FLAG + dfs_full_path_out + "   delete..." + reuslt); 
    	}
    	
    	//------------------------
        Job job = Job.getInstance(conf); 
        job.setJarByClass(indexMerge.class);
        
        job.setMapperClass(indexMergeMapper.class);
        job.setReducerClass(indexMergeReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); 
 
        FileInputFormat.setInputPaths(job, new Path(dfs_path_in));
        FileOutputFormat.setOutputPath(job, new Path(dfs_path_out));  
        
        int res = job.waitForCompletion(true)?0:1 ;  
        String resStr = (res==0)?"OK!":"NG!"; 
		System.out.println( dfsConfig.LOG_FLAG + " output: " + dfs_full_path_out +" ..." + resStr); 
	}
}
