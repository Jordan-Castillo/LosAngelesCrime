/**
  * Created by jtcas on 12/4/2018.
  */


//SCALA IMPORTS
import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
//JAVA IMPORTS
import java.io._

object crimeTransformer {
  def main(args: Array[String]) = {

    System.setProperty("hadoop.home.dir", "c:/winutils/")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("crimeTransformer").setMaster("local[4]")
    val sc = new SparkContext(conf)
    def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _: Throwable => None }

    val incomeLines = sc.textFile("D:\\Eclipse Workspace\\LosAngelesCrime\\src\\FixedData\\income_LA.csv")
      .map(line => ((line.split(",")(0).toDouble, line.split(",")(1).toDouble), line.split(",")(2)))
    val crimeLines = sc.textFile("D:\\Eclipse Workspace\\LosAngelesCrime\\src\\Data\\Crime_Data_from_2010_to_Present.csv")
      .map(line => ((line.split(",")(line.split(",").size - 2), line.split(",")(line.split(",").size - 1)), line))
      .filter(_._1._1.length > 1)
      .filter(_._1._2.length > 1)
      .map({case ((long, lat), line) => ((parseDouble(long.substring(2)), parseDouble(lat.substring(0, lat.size - 2))), line)})
      .filter(_._1._1 != None)
      .filter(_._1._2 != None)
      .map({case ((Some (lat), Some (long)), line) => (((lat.toDouble * 10).round / 10.0, (long.toDouble * 10).round / 10.0), line)})
      /**
        MAKE CHANGES HERE, DEPENDING ON WHAT CRIME DATA YOU ARE LOOKING FOR
      **/
      .groupByKey()
      .join(incomeLines)


      println(crimeLines)
//    crimeLines.take(2).foreach(println(_))
//    crimeLines.foreach(println(_))

  }
}