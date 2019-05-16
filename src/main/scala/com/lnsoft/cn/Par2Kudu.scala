//package com.lnsoft.cn
//
//import java.io.File
//import org.apache.kudu.spark.kudu._
//import org.apache.kudu.client._
//import collection.JavaConverters._
//import org.apache.spark.sql.SparkSession
//
//object Par2Kudu {
//  def main(args: Array[String]): Unit = {
//    val source=args(0)
//    val midlle=args(1)
//    val target=args(2)
//    val warehouseLocation=new File("spark-warehouse").getAbsolutePath
//    val spark=SparkSession.builder().appName("Import Parquet to kudu").config("spark.sql.warehouse.dir",warehouseLocation).enableHiveSupport().getOrCreate()
//
//    val df=spark.read.options(Map("kudu.master"->"qyw-bigdata01:7051,qyw-bigdata03:7051,qyw-bigdata04:7051","kudu.table"->target)).kudu
//    df.createOrReplaceTempView(midlle)
//    import spark.implicits._
//    import spark.sql
//    sql("insert into ".concat(midlle).concat("  select * from  ").concat(source)).show()
//  }
//}
