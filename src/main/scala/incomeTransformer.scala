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

object incomeTransformer {
  def main(args: Array[String]) = {

    //System.setProperty("hadoop.home.dir", "c:/winutils/")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //val pw = new PrintWriter(new File("D:\\Eclipse Workspace\\LosAngelesCrime\\src\\FixedData\\income_LA.csv"))
    val pw = new PrintWriter(new File("resources/income_LA.csv"))

    //val incomeLines = sc.textFile("D:\\Eclipse Workspace\\LosAngelesCrime\\src\\Data\\Income__LA_.csv")
    val incomeLines = sc.textFile("resources/income.csv")
      .map(line => line.split("\""))
      .filter(_.size == 7)
      .map(split => (split(1), split(5)))
      .filter(_._1 != "")
      .filter(_._2 != "")
      .map({case (income, coords) => (income.split(",").mkString("").toInt,
        coords.split(",")(0).substring(1).toDouble,
        coords.split(", ")(1).dropRight(1).toDouble)})
      //.map({case (income, long, lat) => (((long * 10).round / 10.0,(lat * 10).round / 10.0), income)})
      .map({case (income, long, lat) => ((mutateCoords(long), mutateCoords(lat)), income)})
      .aggregateByKey((0,0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2))
      //.map({case ((long, lat), incomeList) => (long, lat, incomeList.reduce(_+_)/incomeList.size)})
      .sortBy(_._1)



    incomeLines.collect().foreach({case ((long, lat), (sum, count)) =>
      pw.write(long.toString + ", " + lat.toString + ", " + (sum*1.0/count).toString + ", " + count + "\n")})
    pw.close()
  }

  // splitting coords to more than 57 (if we want more)
  // method will round all the coords down to the lower 0.05
  def mutateCoords (x : Double): Double = {

    val rounded = (x * 100).round
    return (rounded - (rounded % 5)) / 100.0

    /*val y = (BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_DOWN) * 100).toInt
    val z = BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_DOWN)
    //      val y = (x - BigDecimal(x).setScale(0, BigDecimal.RoundingMode.DOWN).toDouble * 100).round
    y match {
      case it if 0 until 2 contains it => 0.00
    }*/
  }
}