package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  pickupInfo.createOrReplaceTempView("pickupInfo")
  pickupInfo = spark.sql(s"select x,y,z, count(*) as count from pickupInfo WHERE z <= $maxZ AND z >= $minZ AND y <= $maxY AND y >= $minY AND x <= $maxX AND x >= $minX GROUP BY x,y,z")
  pickupInfo.createOrReplaceTempView("pickupInfo")
  
  val total_pickup =  pickupInfo.agg(sum("count")).first.getLong(0)
  val pickup_list = pickupInfo.collect()
  
  var map:Map[(Int,Int,Int),Long] = Map()

  var square_count = 0l
  for (row <- pickup_list) {
    map += ((row.getInt(0),row.getInt(1),row.getInt(2)) -> row.getLong(3))
    square_count += row.getLong(3)*row.getLong(3)
  }

  val mean = total_pickup.toDouble/numCells.toDouble
  val std_dev = Math.sqrt(square_count.toDouble/numCells.toDouble - mean*mean)

  spark.udf.register("calculateGetisOrd", (wijxj:Int, totalNeighbours:Int) => (HotcellUtils.calculateGetisOrd(wijxj, totalNeighbours, std_dev, numCells.toInt, mean)) )
  var getisOrd = spark.sql("select a.x as x,a.y as y,a.z as z, sum(b.count) as wijxj, count(*) as totalNeighbours from pickupInfo as a, pickupInfo as b where b.x>= a.x-1 and b.x<=a.x+1 and b.y>=a.y-1 and b.y<=a.y+1 and b.z>=a.z-1 and b.z<=a.z+1 group by a.x,a.y,a.z")
  getisOrd.createOrReplaceTempView("getisOrdView")
  var getisOrdView = spark.sql("select x,y,z, totalNeighbours, calculateGetisOrd(wijxj, totalNeighbours) as getisOrd from getisOrdView order by getisOrd desc limit 50").persist()

  var result = getisOrdView.drop("totalNeighbours","getisOrd")

  return result

}
}
