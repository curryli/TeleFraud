package sparksql
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import scala.collection.mutable.MutableList
import scala.Range
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Buffer,Set,Map}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.functions._

object tracing {
  

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);

    //    require(args.length == 3)

    val conf = new SparkConf().setAppName("compare_2time")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
   
    val startTime = System.currentTimeMillis(); 
    
    
//    hc.sql(s"set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat" +
//           s"set mapred.max.split.size=10240000000" +
//           s"set mapred.min.split.size.per.node=10240000000" +
//           s"set mapred.min.split.size.per.rack=10240000000" +
//           s"set mapreduce.jobtracker.split.metainfo.maxsize = -1" +
//           s"set mapreduce.job.queuename=root.queue2")
    
 
   //在tbl_common_his_trans中寻找黑名单卡号发生的所有转账交易(一段时间内)。
   var Alldata =  hc.sql(s"select t1.* from tbl_common_his_trans t1 " +
		s"left semi join fraudCardsmd5_testA t2 " +
		s"on t1.tfr_in_acct_no=t2.card " +
		s"where t1.pdate>='20161212' and t1.pdate<='20161212' " +
		s"union all " +
		s"select t1.* from tbl_common_his_trans t1 " +
		s"left semi join fraudCardsmd5_testA t2 " +
		s"on t1.tfr_out_acct_no=t2.card " +
		s"where t1.pdate>='20161212' and t1.pdate<='20161212' " +
		s"union all " +
		s"select t1.* from tbl_common_his_trans t1 " +
		s"left semi join fraudCardsmd5_testA t2 " +
		s"on t1.pri_acct_no_conv=t2.card " +
		s"where t1.pdate>='20161212' and t1.pdate<='20161212' ").cache
   
    Alldata.show(2)
    println( "Count is : " + Alldata.count())
 
    var transferData = Alldata.filter(Alldata("trans_id") === "S33")
    
    var ini_cards_testA = transferData.select("tfr_in_acct_no").unionAll(transferData.select("tfr_out_acct_no"))
    ini_cards_testA.show()
    
  }
   
  
}