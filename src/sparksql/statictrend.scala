package sparksql
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object statictrend {
  

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
    
    
    hc.sql(s"set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat" +
           s"set mapred.max.split.size=10240000000" +
           s"set mapred.min.split.size.per.node=10240000000" +
           s"set mapred.min.split.size.per.rack=10240000000" +
           s"set mapreduce.jobtracker.split.metainfo.maxsize = -1" +
           s"set mapreduce.job.queuename=root.queue3")
    
    
    hc.sql(s"INSERT OVERWRITE TABLE tele_fraud "+
            s"select t1.* from tbl_common_his_trans t1 "+
            s"left semi join fraudCards_md5 t2 "+
            s"on t1.tfr_in_acct_no=t2.card "+
            s"where t1.pdate>='20160701' and t1.pdate<='20161131' "+
            s"union all "+
            s"select t1.* from tbl_common_his_trans t1 "+
            s"left semi join fraudCards_md5 t2 "+
            s"on t1.tfr_out_acct_no=t2.card "+
            s"where t1.pdate>='20160701' and t1.pdate<='20161131' "+
            s"union all "+
            s"select t1.* from tbl_common_his_trans t1 "+
            s"left semi join fraudCards_md5 t2 "+
            s"on t1.pri_acct_no_conv=t2.card "+
            s"where t1.pdate>='20160701' and t1.pdate<='20161131' ")
        
   println("SQL done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
    
 
  }
   
  
}