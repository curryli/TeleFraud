

package sparksql

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext


object Test {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN);
    Logger.getLogger("akka").setLevel(Level.WARN);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.WARN);
 
   
    
    val conf = new SparkConf().setAppName("Anti Search Test")
//    conf.set("driver-class-path", "/opt/cloudera/parcels/CDH/lib/hive/lib/*")
    
    println("start spark Job")
//    val cl = ClassLoader.getSystemClassLoader
//    cl.asInstanceOf[java.net.URLClassLoader].getURLs.foreach(println)
    
    
    
    
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)

    require(args.length == 6)
    val beginDate = args(0)
    val endDate = args(1)
    val tableName=args(2)
    val srcColumn = args(3)
    val destColumn = args(4)
    val srcColumnList = args(5).split(",")
    
    
    
    println("beginDate:\t"+beginDate)
    println("endDate:\t"+endDate)
    println("tableName:\t"+tableName)
    println("srcColumn:\t"+srcColumn)
    println("destColumn:\t"+destColumn)
    println("srcColumnList:\t"+srcColumnList.mkString(","))
    
antiSearch(hc,tableName,beginDate,endDate,srcColumn,destColumn,srcColumnList)
    def antiSearch(hc: HiveContext, tableName: String, beginDate: String, endDate: String, srcColumn: String, destColumn: String, srcColumnList: Array[String]) = {
      var currentSrcDataSize = srcColumnList.length.toLong
      var destDataSize = 0l
      
      var lastSrcDataSize=0l

      val data = hc.sql(s"select " +
        s"trim($srcColumn)  as $srcColumn," +
        s"trim($destColumn) as $destColumn " +
        s"from $tableName " +
        s"where pdate>=$beginDate and pdate<$endDate and " +
        s"sti_takeout_in='1'  and cu_trans_st='10000' and resp_cd1='00' and " +
        s"trim($srcColumn) !='' and trim($destColumn) !='' "+
        s"group by trim($srcColumn),trim($destColumn)").repartition(100).cache()
        
      var tempData=null 
      var srcColumnData=srcColumnList.mkString("','")
      println(srcColumnData.toString())
      var i=0
      while(lastSrcDataSize!=currentSrcDataSize){
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

}