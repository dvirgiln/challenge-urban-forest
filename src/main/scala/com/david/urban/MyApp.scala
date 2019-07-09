package com.david.urban
import au.com.eliiza.urbanforest.{Loop, MultiPolygon, Point, Polygon}
import au.com.eliiza.urbanforest.mayIntersect
import au.com.eliiza.urbanforest.intersectionArea
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec

object MyApp extends App {

  case class ForestArea(area: Double, polygons: MultiPolygon)
  case class Area(id: String, polygons: MultiPolygon)
  // Create a SparkSession. No need to create SparkContext
  // You automatically get it as part of the SparkSession
  val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
  val spark = SparkSession
    .builder()
    .appName("SparkSessionZipsExample")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .master("local[8]")
    .getOrCreate()
  import spark.sqlContext.implicits._
  val sparkContext = spark.sparkContext
  val urbanForestRDD = sparkContext.textFile("src/main/resources/melb_urban_forest_2016.txt/part-00[0-5]*")

  @tailrec
  def  fromListDoublesToLoop(list: List[Double], result: List[Point]): List[Point] =  list match {
    case Nil => result
    case a :: b :: tail =>  fromListDoublesToLoop(tail, Seq(a, b) :: result)
  }
  val fromStringToDoubles = (s: String) => {
    s.filter(a => !a.equals('(') && !a.equals(')')).split(",")
      .map{_.trim.split(" ").map(a => a.filterNot(_.equals('\"')).toDouble)}
      .map(_.toList)
  }
  val convertToPolygon = (s: String) => fromStringToDoubles(s).map(a => fromListDoublesToLoop(a, Nil).toSeq)
  val forestAreaRDD = urbanForestRDD.map(line => line.split(" ").toList).map(l => (l.head.filterNot(_.equals('\"')).toDouble, l.tail.tail.reduce(_ + " " + _)))

  // As the data for the forest areas is not formatted in a standard format, then we need to parse it with RDDs
  val forestPolygonRDD = forestAreaRDD.map{ case (area, coordinates) => (area, convertToPolygon(coordinates))}.map(a => ForestArea(a._1, Seq(a._2.toSeq)))
  val forestPolygonDS= forestPolygonRDD.toDS()
  forestPolygonDS.createOrReplaceTempView("forest_area")

  val areasDF=spark.sqlContext.read.json("src/main/resources/melb_inner_2016.json")
  areasDF.createOrReplaceTempView("full_areas")
  areasDF.printSchema()
  val reducesAreasDS = spark.sql("SELECT sa1_main16 as id, geometry.coordinates as polygons FROM full_areas").as[Area]
  reducesAreasDS.createOrReplaceTempView("area")
  import org.apache.spark.sql.functions.udf

  val mayIntersectUDF = udf((x: MultiPolygon, y: MultiPolygon) => mayIntersect(x, y))

  /*val join = reducesAreasDS.select('id as "area_id", 'polygons as "area_polygons").crossJoin(forestPolygonDS.select('polygons as "forest_polygons"))
  println(s"Join Areas=${join.count()}")
  val possibleForestAreas = join.filter(mayIntersectUDF(join("area_polygons"),join("forest_polygons")))
  println(s"possibleForestAreas Areas=${possibleForestAreas.count()}")*/
  val possibleForestAreas = reducesAreasDS.join(forestPolygonDS, mayIntersectUDF(reducesAreasDS("polygons"),forestPolygonDS("polygons")))

  val intersectionAreaUDF = udf((x: MultiPolygon, y: MultiPolygon) => intersectionArea(x, y))
  possibleForestAreas.cache()
  println(s"Number of Possible Forest Areas=${possibleForestAreas.count()}")
  val forestAreasDF=possibleForestAreas.withColumn("area",intersectionAreaUDF(possibleForestAreas("area_polygons"), possibleForestAreas("forest_polygons")))

  import org.apache.spark.sql.functions._
  val sortedForestAreas=forestAreasDF.sort(desc("area")).select("area", "area_id")

  sortedForestAreas.show(10)


}
