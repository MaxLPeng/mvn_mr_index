maven工程  
MapReduce API调用实验  

文字在不同文件中的次数统计 ，纵向  
com.pl.index.indexWord  

#数据源文件内容  
 index01.txt  
    hello	tom  
    hello	jerry  
    hello	jerry  
 index02.txt  	
   hello	jerry  
   jerry	tom  
 index03.txt  
   hello	jerry   
 
#数据源文件 数据特征  
字符 文件名 数量  
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
#运行结果  
	hello-->index01.txt	3  
	hello-->index02.txt	1  
	hello-->index03.txt	1  
	jerry-->index01.txt	2  
	jerry-->index02.txt	2  
	jerry-->index03.txt	1  
	tom-->index01.txt	1  
	tom-->index02.txt	1  


文字在不同文件中的次数统计 ，横向  
com.pl.index.indexMerge  
#输入 
	hello-->index01.txt	3  
	hello-->index02.txt	1  
	hello-->index03.txt	1  
	jerry-->index03.txt	1  
	jerry-->index01.txt	2  
	jerry-->index02.txt	2  
	tom-->index01.txt	1  
	tom-->index02.txt	1  
  
#运行结果  
hello	index03.txt-->1	index02.txt-->1	index01.txt-->3  
jerry	index03.txt-->1	index02.txt-->2	index01.txt-->2  
tom	index02.txt-->1	index01.txt-->1  
