# Web-Log-Analysis
网站基本指标案例分析(MapReduce)

# 指标
PV:网页被浏览的次数(Page view)

UV:独立访客数(unique visitor)

VV:访客的访问次数(visitor view)

IP:独立的IP数

## hadoop生态系统
Hadoop
>数据的处理:MapReduce  
>数据的存储:hdfs

MapReduce只适用于离线批处理，编程过程较为复杂
>如果想处理更快，使用内存，诞生:spark  
>实时处理:storm

简单编程框架:SQL
>hive框架不计算，调用计算框架  
impala:有自已的计算逻辑

HDFS的存储效率
>tacyon:应用内存式的存储  
nosql:作为缓存  
rdbms:实际存储

对于不同框架间的任务调度
>zeus  
oozie  
azkaban  

基于不同数据源的数据采集
>文件  
关系型数据库  
nosql  

监控平台
>CM clousar manager  
Ambari:HDP
