package sparkbyexample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.CurrentTimestamp

object rddtransformations {
  
  def main(args:Array[String]):Unit={
    
    val spark = SparkSession.builder()
                .appName("sparkbyexample")
                .master("local[*]")
                .getOrCreate()
                
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    val data = sc.textFile("src/main/resources/test.txt")
    //data.foreach(println)
    //println(data.getNumPartitions)
    
    val datarepart = data.repartition(4)
    //println(datarepart.getNumPartitions)
    
    //datarepart.collect().foreach(println)
    val dataflatmap = datarepart.flatMap(x => x.split(" "))
    //dataflatmap.foreach(println)
    
    val datamap = dataflatmap.map(x => (x,1))
    //datamap.foreach(println)
    
    val datafilter = datamap.filter(x => x._1.startsWith("a"))
    //datafilter.foreach(println)
    
    val datareducebykey = datafilter.reduceByKey(_+_)
    //datareducebykey.foreach(println)
    
    val dataswapwords = datareducebykey.map(x => (x._2,x._1))
    //dataswapwords.foreach(println)
    
    val datasort = dataswapwords.sortByKey()
    //datasort.foreach(println)
    
    val datacount = datasort.count()
    //println(datacount)
    
    val datafirst = datasort.first()
    //println(datafirst._1,datafirst._2)
    
    val datareduce = datasort.reduce((a,b) => (a._1 + b._1,a._2))
    //println(datareduce)
    
    val datatake = datasort.take(3)
    //datatake.foreach(println)
    
    //datafilter.repartition(1).saveAsTextFile("src/main/resource/output1.txt")
    
    
    

    
  }
  
}