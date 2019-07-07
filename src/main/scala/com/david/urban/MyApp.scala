package com.david.urban
import au.com.eliiza.urbanforest.{Polygon, Loop, Point}
import org.apache.spark.sql.SparkSession

object MyApp extends App {
  // Create a SparkSession. No need to create SparkContext
  // You automatically get it as part of the SparkSession
  val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
  val spark = SparkSession
    .builder()
    .appName("SparkSessionZipsExample")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .master("local[8]")
    .getOrCreate()
  val sparkContext = spark.sparkContext
  val urbanForestRDD = sparkContext.textFile("melb_urban_forest_2016.txt/part-00[0-5]*")

  def  fromListDoublesToLoop(list: List[Double]): List[Point] =  list match {
    case Nil => Nil
    case a :: b :: tail => Seq(a, b) :: fromListDoublesToLoop(tail)
  }
  val fromStringToDoubles = (s: String) => s.filter(a => !a.equals('(') && !a.equals(')')).split(",").map(l => l.trim.split(" ").map(_.filterNot(_.equals('\"')).toDouble)).map(_.toList)
  val convertToPolygon = (s: String) => fromStringToDoubles(s).map(a => fromListDoublesToLoop(a).toSeq)
  val forestAreaRDD = urbanForestRDD.map(line => line.split(" ").toList).map(l => (l.head.filterNot(_.equals('\"')).toDouble, l.tail.tail.reduce(_ + _)))
  val forestPolygonRDD = forestAreaRDD.map{ case (area, coordinates) => (area, convertToPolygon(coordinates))}
  val output = forestPolygonRDD.take(2)
  println(output.toList)
}
