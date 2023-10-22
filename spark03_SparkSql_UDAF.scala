package spark_sql


import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

object spark03_SparkSql_UDAF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.read.json("input/emp.json")

    df.createOrReplaceTempView("emp")

//    spark.udf.register("prefixName",(name:String)=>{
//      "Name:"+name
//    })

    spark.udf.register("ageAvg",functions.udaf(new MyAvgUDAF()))



    spark.sql("select ageAvg(age) ageAvg from emp").show()

    spark.stop()
  }

  /*
  *   IN :输入的数据类型
  *   buf
  *   OUT :输出的数据类型
  *
   */
  case  class  Buf(var total:Long,var count:Long)

  class MyAvgUDAF extends Aggregator[Long,Buf,Long]{
    //zero 初始值或者零值
    //缓冲区的初始化
    override def zero: Buf = {
      Buf(0L,0L)
    }
    //输入的数据更新缓冲区的数据
    override def reduce(buf: Buf, in: Long): Buf = {
      buf.total=buf.total+in
      buf.count=buf.count+1
      buf

    }

    //合并缓存区
    override def merge(buf1: Buf, buf2: Buf): Buf = {
      buf1.total=buf1.total+buf2.total
      buf1.count=buf1.count+buf2.count
      buf1
    }


    //计算结果
    override def finish(reduction: Buf): Long = {
      reduction.total/reduction.count
    }

    //缓存区的编码操作
    override def bufferEncoder: Encoder[Buf] = Encoders.product

    //输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}


