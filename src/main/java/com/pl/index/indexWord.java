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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.pl.index.dfsConfig.h_mode;

 
 
/*
 * 实现功能 ：字符在文件里的出现次数的索引
 * 
 =======================================
 数据源文件内容
 index01.txt
		hello	tom
		hello	jerry
		hello	jerry 
 index02.txt		
		hello	jerry
		jerry	tom 
 index03.txt
 		hello	jerry 
 
 =======================================
数据源文件 数据特征  
           字符           文件名                 数量
	hello-->index01.txt	3
	hello-->index02.txt	1
	hello-->index03.txt	1
	jerry-->index03.txt	1
	jerry-->index01.txt	2
	jerry-->index02.txt	2
	tom  -->index01.txt	1
	tom  -->index02.txt	1

    hello  index01.txt->3,index02.txt->1,index03.txt->1
    tom    index01.txt->1,index02.txt->1
    jerry  index01.txt->2,index02.txt->1,index03.txt->1  
    
 =======================================
运行结果
	hello-->index01.txt	3
	hello-->index02.txt	1
	hello-->index03.txt	1
	jerry-->index01.txt	2
	jerry-->index02.txt	2
	jerry-->index03.txt	1
	tom-->index01.txt	1
	tom-->index02.txt	1
 */



/**
 * 倒排索引
 * @author max400
 *
 */
public class indexWord {

	public static class indexwordMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException { 
			
			//切分单词
			String lineInfo = value.toString();
			String[] words = lineInfo.split("\t");
			
			//获取该行所在文件切片
			FileSplit  inputSplit = (FileSplit)context.getInputSplit();
			String filename = inputSplit.getPath().getName(); 
			
			for(String word : words) {
				//kv输出格式：  key=[hello-->index01.txt]	value=[1]
	            context.write(new Text(word+"-->"+filename), new LongWritable(1)); 
	        } 
			/*  
			输出：拆分出所有字符在各文件的情况
			[key]  value=1
			[hello-->index01.txt] 1
			[tom-->index01.txt] 1
			[hello-->index01.txt] 1
			[jerry-->index01.txt] 1
			[hello-->index01.txt] 1
			[jerry-->index01.txt] 1
			
			[hello-->index02.txt] 1
			[jerry-->index02.txt] 1
			[jerry-->index02.txt] 1
			[tom-->index02.txt]   1
			
			[hello-->index03.txt] 1
			[jerry-->index03.txt] 1
			 */
		} 
	}
	
	public static class indexwordReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		
		/* 
		 输入：
		  	[[[[[
		  	hello-->index01.txt 	{1,1,1}  
		  	tom-->index01.txt 		{1} 
		  	jerry-->index01.txt 1 	{1,1} 
		  
		  	[[[[[
			hello-->index02.txt 1	{1} 
		  	jerry-->index02.txt 1	{1,1}  
		  	tom-->index02.txt 1		{1}
		  	
			[[[[[
			hello-->index03.txt 1	{1} 
		  	jerry-->index03.txt 1	{1}
		 */ 
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) 
				throws IOException, InterruptedException { 

			long count = 0;
			for	(LongWritable v:values) {
					count +=v.get();  
			}
			context.write(key, new LongWritable(count));
			
		}
	}
	
	static String dfs_path_in = dfsConfig.DFS_PATH_IN + "/index";
	static String dfs_path_out = dfsConfig.DFS_PATH_OUT + "/index";
	static String dfs_full_path_in = dfsConfig.DFS_FULL_PATH_IN + "/index";
	static String dfs_full_path_out = dfsConfig.DFS_FULL_PATH_OUT + "/index";
	
	public static void main(String[] args) throws Exception {
		
		System.out.println(dfsConfig.LOG_FLAG + dfs_path_in);
		System.out.println(dfsConfig.LOG_FLAG + dfs_path_out);
		System.out.println(dfsConfig.LOG_FLAG + dfs_full_path_in);
		System.out.println(dfsConfig.LOG_FLAG + dfs_full_path_out);
		
		//配置服务器 
        Configuration conf = dfsConfig.getConf(h_mode.HA);
        //删除历史输出目录
        FileSystem fileSystem = FileSystem.get(conf); 
    	boolean reuslt= fileSystem.exists(new Path(dfs_path_out));
    	if (reuslt) {
	    	reuslt = fileSystem.delete(new Path(dfs_path_out), true);
			System.out.println(dfsConfig.LOG_FLAG + dfs_full_path_out + "   delete..." + reuslt); 
    	}    	
    	//------------------------
        Job job = Job.getInstance(conf); 
        job.setJarByClass(indexWord.class);
        
        job.setMapperClass(indexwordMapper.class);
        job.setReducerClass(indexwordReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class); 
 
        FileInputFormat.setInputPaths(job, new Path(dfs_path_in));
        FileOutputFormat.setOutputPath(job, new Path(dfs_full_path_out));  
        
        int res = job.waitForCompletion(true)?0:1 ;  
        String resStr = (res==0)?"OK!":"NG!"; 
		System.out.println( dfsConfig.LOG_FLAG + " output: " + dfs_full_path_out +" ..." + resStr); 
	}
}
