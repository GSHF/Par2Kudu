package com.lnsoft.cn

import java.io.File
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._
import collection.JavaConverters._
import org.apache.spark.sql.SparkSession

object Kudu2Par {
def main(args: Array[String]): Unit = {
    val kuduTable=args(0)
    val parTable=args(1)
    val warehouseLocation=new File("spark-warehouse").getAbsolutePath
    val spark=SparkSession.builder().appName("Import Kudu  to Parquet").config("spark.sql.warehouse.dir",warehouseLocation).enableHiveSupport().getOrCreate()

    val df=spark.read.options(Map("kudu.master"->"bigdata-master02:7051,bigdata-master04:7051,bigdata-master05:7051","kudu.table"->kuduTable)).kudu.select("*")
//    df.createOrReplaceTempView(midlle) val midlle=args(1)
    import spark.implicits._
    import spark.sql
//    sql("insert into ".concat(parTable).concat("  select * from  ").concat(midlle)).show()
    df.write.parquet("hdfs://10.193.2.33:8020/user/hive/warehouse/qianyi.db/".concat(parTable))
}
}
