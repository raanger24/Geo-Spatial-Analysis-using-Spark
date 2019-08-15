package cse512

import org.apache.spark.sql.SparkSession
import math.{sqrt, pow}

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((

    {
        val rectangle = queryRectangle.split(",")
        val x1 = rectangle(0).toDouble
        val y1 = rectangle(1).toDouble
        val x2 = rectangle(2).toDouble
        val y2 = rectangle(3).toDouble

        var xMax = 0.0
        var xMin = 0.0
        var yMax = 0.0
        var yMin = 0.0

        if (x1 > x2) {
            xMax = x1
            xMin = x2
        } else {
            xMax = x2
            xMin = x1
        }
        if (y1 > y2) {
            yMax = y1
            yMin = y2
        } else {
            yMax = y2
            yMin = y1
        } 

        val point = pointString.split(",")
        val x_coord = point(0).toDouble
        val y_coord = point(1).toDouble

        if (x_coord >= xMin && x_coord <= xMax && y_coord >= yMin && y_coord <= yMax) {
            true
        } else {
            false
        }
    }

    )))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((

    {
        val rectangle = queryRectangle.split(",")
        val x1 = rectangle(0).toDouble
        val y1 = rectangle(1).toDouble
        val x2 = rectangle(2).toDouble
        val y2 = rectangle(3).toDouble

        var xMax = 0.0
        var xMin = 0.0
        var yMax = 0.0
        var yMin = 0.0

        if (x1 > x2) {
            xMax = x1
            xMin = x2
        } else {
            xMax = x2
            xMin = x1
        }
        if (y1 > y2) {
            yMax = y1
            yMin = y2
        } else {
            yMax = y2
            yMin = y1
        } 

        val point = pointString.split(",")
        val x_coord = point(0).toDouble
        val y_coord = point(1).toDouble

        if (x_coord >= xMin && x_coord <= xMax && y_coord >= yMin && y_coord <= yMax) {
            true
        } else {
            false
        }
    }

    )))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((

    {
        val point1 = pointString1.split(",")
        val x1 = point1(0).toDouble
        val y1 = point1(1).toDouble
        
        val point2 = pointString2.split(",")
        val x2 = point2(0).toDouble
        val y2 = point2(1).toDouble

        var d = sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))

        if (d < distance) {
            true
        } else {
            false
        }
    }

    )))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((

    {
        val point1 = pointString1.split(",")
        val x1 = point1(0).toDouble
        val y1 = point1(1).toDouble
        
        val point2 = pointString2.split(",")
        val x2 = point2(0).toDouble
        val y2 = point2(1).toDouble

        var d = sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))

        if (d < distance) {
            true
        } else {
            false
        }
    }

    )))

    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
