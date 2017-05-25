package sparksql

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel


object CardsTransferTracing {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);
 
   
    
    val conf = new SparkConf().setAppName("Anti Search Test")
//    conf.set("driver-class-path", "/opt/cloudera/parcels/CDH/lib/hive/lib/*")
    
    println("start spark Job")

    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)

    //require(args.length == 6)
    val beginDate = "20160701"
    val endDate = "20160931"
    val tableName= "tbl_common_his_trans"
    val srcColumn = "tfr_in_acct_no"
    val destColumn = "tfr_out_acct_no"
    
    
    val srcColumnList = sc.textFile("xrli/TeleFraud/fraudcardsmd5_2.txt").collect() 
    
    
//    val srcColumnList = Array("168f13cc685b87c4dc3cf4f6eb5f0302",
//                              "f1ad01636dd9f12de1803528fa5aa207",
//                              "25adeeb9a836a5c3b1a4169a21bc0395",
//                              "16a284521d06eda1d8358a49b27c6ae8",
//                              "8cf2840e66bdd40b70da47ee6ead4dd2",
//                              "90fee82f8c179967fa6c16019ca3570b",
//                              "566172cf3aeec85c2827946735853a46")
    

 
    
    antiSearch(hc,tableName,beginDate,endDate,srcColumn,destColumn,srcColumnList)
    
  }
  
  
    def antiSearch(hc: HiveContext, tableName: String, beginDate: String, endDate: String, srcColumn: String, destColumn: String, srcColumnList: Array[String]) = {
      val maxitertimes =2
      var currentSrcDataSize = srcColumnList.length.toLong
      var destDataSize = 0L
      
      var lastSrcDataSize=0L

      val data = hc.sql(s"select " +
        s"trim($srcColumn)  as $srcColumn," +
        s"trim($destColumn) as $destColumn " +
        s"from $tableName " +
        s"where pdate>=$beginDate and pdate<$endDate and " +
        s"trim($srcColumn) !='' and trim($destColumn) !='' "+
        s"group by trim($srcColumn),trim($destColumn)").repartition(100).cache()
        
      var tempData=null 
      var srcColumnData=srcColumnList.mkString("','")
      println(srcColumnData.toString())
      var i=0
      
      while(i<maxitertimes && lastSrcDataSize!=currentSrcDataSize){
       i=i+1
       println("Start iteration " + i)
       var tempData= data.filter(s"${srcColumn} in (\'${srcColumnData}\') or "+
                                 s"${destColumn} in (\'${srcColumnData}\')").
             select(s"${srcColumn}",s"${destColumn}").distinct()
             
       var dataFrame1=tempData.select(s"${srcColumn}").distinct()
       var dataFrame2=tempData.select(s"${destColumn}").distinct() 
       var temp= dataFrame1.unionAll(dataFrame2).distinct().map { r => r.getString(0) }
       
       lastSrcDataSize=currentSrcDataSize
       currentSrcDataSize=temp.count()
       srcColumnData=temp.collect().mkString("','")
       println(s"${srcColumn} Data Count:\t"+currentSrcDataSize)
       println(srcColumnData)
      }
      
     
    }
 

}