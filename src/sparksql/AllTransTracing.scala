package sparksql

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel


object AllTransTracing {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);
 
       //require(args.length == 6)
    val beginDate = "20160828"
    val endDate = "20160828"
    val tableName= "tbl_common_his_trans"
    val srcColumn = "tfr_in_acct_no"
    val destColumn = "tfr_out_acct_no"
    
    val conf = new SparkConf().setAppName("Anti Search Test")
//    conf.set("driver-class-path", "/opt/cloudera/parcels/CDH/lib/hive/lib/*")
    
    println("start spark Job")

    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)


    
    hc.sql(s"set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat" +
           s"set mapred.max.split.size=10240000000" +
           s"set mapred.min.split.size.per.node=10240000000" +
           s"set mapred.min.split.size.per.rack=10240000000" +
           s"set mapreduce.jobtracker.split.metainfo.maxsize = -1" +
           s"set mapreduce.job.queuename=root.spark")
           
    //val seedList = sc.textFile("xrli/TeleFraud/fraudCards_1.txt").collect() 
    
    
    val seedList = Array("8127fd4b2286c136abef77fb18100ef0",
                         "777d85a415bed7576491de0938b33c76",
                         "8e6fb529da1d75468846d9feabe0293b",
                         "73502834a5f0524f576b49cf37d75bf1",
                         "538d8a8e4314eb5cfe539e3935a77223",
                         "c054034ec59c747c481e170622c93bb8")
    
    antiSearch(hc,tableName,beginDate,endDate,srcColumn,destColumn,seedList)
    
  }
  
  
    def antiSearch(hc: HiveContext, tableName: String, beginDate: String, endDate: String, srcColumn: String, destColumn: String, seedList: Array[String]) = {
      val maxitertimes =3
      var transferList = seedList
      var currentSrcDataSize = transferList.length.toLong
      var destDataSize = 0L
      
      var lastSrcDataSize=0L

      var data = hc.sql(s"select pri_acct_no_conv, trans_id, trans_at, pdate, loc_trans_tm, acpt_ins_id_cd, trans_md, cross_dist_in,trans_id, " +
        s"trim($srcColumn)  as $srcColumn," +
        s"trim($destColumn) as $destColumn " +
        s"from $tableName " +
        s"where pdate>=$beginDate and pdate<=$endDate").repartition(100).cache()
        
      var transferCards_tmp=null 
      var seedData=transferList.mkString("','")
      println(seedData.toString())
      var i=0
      
      while(i<maxitertimes && lastSrcDataSize!=currentSrcDataSize){
       i=i+1
       println("Start iteration " + i)
       //var cosumeData_tmp= data.filter(s"pri_acct_no_conv in (\'${seedData}\') ")
       //https://stackoverflow.com/questions/36562678/sparks-column-isin-function-does-not-take-list
       var cosumeData_tmp= data.filter(data("pri_acct_no_conv").isin(transferList : _*))
            
       println(s"cosumeData_tmp Count:\t"+cosumeData_tmp.count)
       cosumeData_tmp.show(100)
       
       
       var a= data.filter(data(srcColumn).isin(transferList : _*) ||  data(destColumn).isin(transferList : _*))                
       println(s"a Count:\t"+a.count)
       a.show(100)
       
       
       var transferData_tmp= data.filter(data("trans_id")==="S33" && 
           (data(srcColumn).isin(transferList : _*) ||  data(destColumn).isin(transferList : _*)))
                                   
       println(s"transferData_tmp Count:\t"+transferData_tmp.count)
       transferData_tmp.show(100)
 
       var transferCards_tmp = transferData_tmp.select(s"${srcColumn}",s"${destColumn}").distinct()
              
       var dataFrame1=transferCards_tmp.select(s"${srcColumn}").distinct()
       var dataFrame2=transferCards_tmp.select(s"${destColumn}").distinct() 
       var cardRdd_transfer= dataFrame1.unionAll(dataFrame2).distinct().map { r => r.getString(0) }
       
       
       lastSrcDataSize=currentSrcDataSize
       currentSrcDataSize=cardRdd_transfer.count()
       println(s"current seed cards Count:\t"+currentSrcDataSize)
       seedData=cardRdd_transfer.collect().mkString(",")
       println(seedData)
       
       transferList = cardRdd_transfer.collect()
      }
      
      data.unpersist(blocking=false)
    }
 

}