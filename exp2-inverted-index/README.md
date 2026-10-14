# 实验二：文档倒排索引算法实现

- **学号**：202400390011
- **姓名**：武庆阳
- **课程**：大数据管理与分析
- **实验日期**：2026-10-14
- **环境**：WSL2 Ubuntu + Hadoop 3.3.5 + OpenJDK 11

## 实验内容

使用Hadoop MapReduce实现文档倒排索引（Inverted Index），输出格式为：
word <doc1,count1>;<doc2,count2>;...;<total,N>



## 文件说明

| 文件 | 说明 |
|------|------|
| `src/InvertedIndex.java` | MapReduce倒排索引核心代码（含Mapper/Combiner/Reducer） |
| `stopwords.txt` | 英文停词表（169个常见停词） |
| `input/` | 英文文档输入数据（来自Project Gutenberg） |

## 运行方法

### 1. 启动Hadoop
```bash
start-dfs.sh
start-yarn.sh
jps

2. 上传数据到HDFS
bash

hdfs dfs -mkdir -p /user/hadoop/exp4/input
hdfs dfs -put input/*.txt /user/hadoop/exp4/input/
hdfs dfs -put stopwords.txt /user/hadoop/exp4/
3. 编译打包
bash

export HADOOP_CLASSPATH=$(hadoop classpath)
javac -classpath $HADOOP_CLASSPATH -d classes src/InvertedIndex.java
jar -cvf inverted-index.jar -C classes/ .
4. 运行MapReduce作业
bash

hadoop jar inverted-index.jar InvertedIndex \
    /user/hadoop/exp4/input \
    /user/hadoop/exp4/output \
    hdfs://localhost:9000/user/hadoop/exp4/stopwords.txt
5. 查看结果
bash

hdfs dfs -cat /user/hadoop/exp4/output/part-r-00000 | head -50
核心设计
Mapper：逐行分词，过滤停词和纯数字，输出 (word, docId)
Combiner：Map端局部聚合，统计每个文档中的词频
Reducer：全局聚合，合并所有分区结果，构建最终倒排索引格式
分布式缓存：通过 job.addCacheFile() 加载停词表到各Mapper内存
实验结果
输入：12个英文文档（来自Project Gutenberg）
输出：约3.4MB倒排索引文件
格式验证：完全符合 <doc,count>;<total,N> 要求
作业状态：FINISHED / SUCCEEDED
