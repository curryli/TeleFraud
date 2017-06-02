package sparksql

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.DataFrame


object TransferCardsTracing {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);
 
       //require(args.length == 6)
    val beginDate = "20160701"
    val endDate = "20161131"
    val tableName= "tbl_common_his_trans"
    val srcColumn = "tfr_in_acct_no"
    val destColumn = "tfr_out_acct_no"
    
    val conf = new SparkConf().setAppName("TransferCardsTracing_cache")
//    conf.set("driver-class-path", "/opt/cloudera/parcels/CDH/lib/hive/lib/*")
    
    println("start spark Job")

    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
 
    hc.sql(s"set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat" +
           //s"set mapred.max.split.size=10240000000" +
           s"set mapred.min.split.size.per.node=10240000000" +
           s"set mapred.min.split.size.per.rack=10240000000" +
           s"set mapreduce.jobtracker.split.metainfo.maxsize = -1" +
           s"set mapreduce.job.queuename=root.spark")
            
    val seedList = sc.textFile("xrli/TeleFraud/seedcards.txt").collect()        
  
    antiSearch(hc,tableName,beginDate,endDate,srcColumn,destColumn,seedList)
    
  }
  
  
    def antiSearch(hc: HiveContext, tableName: String, beginDate: String, endDate: String, srcColumn: String, destColumn: String, seedList: Array[String]) = {
      val maxitertimes =4
      var transferList = seedList
      var currentSrcDataSize = transferList.length.toLong
      var destDataSize = 0L
      var lastSrcDataSize=0L

      val startTime = System.currentTimeMillis(); 
 
      val data = hc.sql(s"select trans_at, loc_trans_tm " +
        s"trim($srcColumn)  as $srcColumn," +
        s"trim($destColumn) as $destColumn " +
        s"from $tableName " +
        s"where pdate>=$beginDate and pdate<=$endDate and "+
        s"trim($srcColumn) !='' and trim($destColumn) !='' ").repartition(100).persist(StorageLevel.MEMORY_AND_DISK_SER)    //.cache
         
        
      println("SQL done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
      
      var transferCards_tmp=null 
      var seedData=transferList.mkString("','")
      println(seedData.toString())
      var i=0
      
      var cosumeData_tmp: DataFrame = null
      var transferData_tmp: DataFrame = null

    while(i<maxitertimes && lastSrcDataSize!=currentSrcDataSize){
       i=i+1
       println("Start iteration " + i)
      
       //select tfr_in_acct_no from tbl_common_his_trans where pdate=20160701 and tfr_in_acct_no in ('497cb45fd4db4845ec7ee9e50f5b5ed1,c9a9e53167e29a61c94de39b93c3f735,ddcef865fe799f16ddfc1a0299ca1a56');
       
       transferData_tmp= data.filter(s"${srcColumn} in (\'${seedData}\') or "+
                                 s"${destColumn} in (\'${seedData}\')").
             select(s"${srcColumn}",s"${destColumn}").distinct()
                               
       println("transferData_tmp filter done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
        
       var dataFrame1=transferData_tmp.select(s"${srcColumn}").distinct()
       var dataFrame2=transferData_tmp.select(s"${destColumn}").distinct() 
       //var cardRdd_transfer= dataFrame1.unionAll(dataFrame2).distinct().rdd.map {_.getString(0)}.persist(StorageLevel.MEMORY_AND_DISK_SER)
       var cardRdd_transfer= dataFrame1.unionAll(dataFrame2).distinct().map{_.getString(0)}//.persist(StorageLevel.MEMORY_AND_DISK_SER)
 
       println("cardRdd_transfer done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
       
       lastSrcDataSize=currentSrcDataSize
       currentSrcDataSize=cardRdd_transfer.count()
       println(s"current seed cards Count:\t"+currentSrcDataSize)
       
       seedData=cardRdd_transfer.collect().mkString(",")
       println(seedData)
       
       transferList = cardRdd_transfer.collect()
       seedData=transferList.mkString("','")
       println("New transferList done in " + (System.currentTimeMillis()-startTime)/(1000*60) + " minutes." )  
      }
       
      transferData_tmp.rdd.saveAsTextFile("xrli/TeleFraud/transferData_0711_cache")
     // data.unpersist(blocking=false)
    }
 

}