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

    System.setProperty("hadoop.home.dir", "c:/winutils/")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val pw = new PrintWriter(new File("D:\\Eclipse Workspace\\LosAngelesCrime\\src\\FixedData\\income_LA.csv"))


    def mutateCoords (x : Double) = {
        val y = (BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_DOWN) * 100).toInt
        val z = BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_DOWN)
//      val y = (x - BigDecimal(x).setScale(0, BigDecimal.RoundingMode.DOWN).toDouble * 100).round
      y match {
        case it if 0 until 2 contains it => 0.00
      }
    }

    val incomeLines = sc.textFile("D:\\Eclipse Workspace\\LosAngelesCrime\\src\\Data\\Income__LA_.csv")
      .filter(_.split(",").size == 15)
      .map(line => (line.split(",")(4), line.split(",")(11).substring(2), line.split(",")(12).substring(0, line.split(",")(12).length - 2)))
      .filter(_._1 != "")
      .map({case (income, long, lat) => (income.toInt, long.toDouble, lat.toDouble)})
      .map({case (income, long, lat) => (((long * 10).round / 10.0,(lat * 10).round / 10.0), income)})
      .groupByKey()
      .map({case ((long, lat), incomeList) => (long, lat, incomeList.reduce(_+_)/incomeList.size)})

    incomeLines.collect().foreach({case (long, lat, incomeAv) => pw.write(long.toString + "," + lat.toString + "," + incomeAv.toString + "\n")})
//    storeLines.collect().foreach(println(_))
    pw.close()
  }
}