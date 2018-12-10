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

    //System.setProperty("hadoop.home.dir", "c:/winutils/")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("crimeTransformer").setMaster("local[4]")
    val sc = new SparkContext(conf)

//    29 commas ordinarily
//    untouched, 1872170 lines of data
    def parseDouble(s: String) = try { if(s.toDouble != 0.0) Some(s.toDouble) else None } catch { case _: Throwable => None }

    //val incomeLines = sc.textFile("D:\\Eclipse Workspace\\LosAngelesCrime\\src\\FixedData\\income_LA.csv")
    val incomeLines = sc.textFile("resources/income_LA.csv")
      .map(line => line.split(", "))
      .map(split => ((split(0).toDouble, split(1).toDouble), split(2).toDouble.round))
    // ((lat, long), averageIncome)

    //val crimeLines = sc.textFile("D:\\Eclipse Workspace\\LosAngelesCrime\\src\\Data\\Crime_Data_from_2010_to_Present.csv")
    val crimeLines = sc.textFile("resources/crime.csv")
      .map(line => line.split(","))
      .map(split => ((split(split.size - 2), split(split.size - 1)), split.mkString(",")))
      .filter(_._1._1.length > 1)
      .filter(_._1._2.length > 1)
      .map({case ((long, lat), line) => ((parseDouble(long.substring(2)), parseDouble(lat.substring(0, lat.size - 2))), line)})
      .filter(_._1._1 != None)
      .filter(_._1._2 != None)
      // if we want to map to lower tenth:
      .map({case ((Some (long), Some (lat)), line) => (((long.toDouble * 10).round / 10.0, (lat.toDouble * 10).round / 10.0), line)})
      // if we want to map to lower 0.05
      //.map({case ((Some (long), Some (lat)), line) => ((mutateCoords(long), mutateCoords(lat)), line)})


    // now have all the ((lat, long), lineWithDetail) in the RDD
    // can just map to whatever split we want
    //   TO GROUP AND SORT: crimeLines.groupByKey().sortBy(_._1)
    // TO JOIN WITH INCOME: (...).join(incomeLines)



    // crimeCodes has each (long, lat) pair with each (codeDesc, count) pair for every code found in that grid square
    val crimeCode = crimeLines.map({case (coord, line) => (coord, line.split(",")(8))})
      .filter({case (coord, code) => code != ""})
      // have ((lat, long), crimeCode)
      .map((_, 1))
      .reduceByKey(_+_)
      .sortBy(r => r._2, false)
      .map({case (((lat, long), code), count) => ((lat, long), (code, count))})
      .groupByKey()  // comment out for OPTION 2
      .sortByKey()
    // ((lat, long), (crimeCode, crimeCount)) sorted by lat and long


    // OPTION 1: IF WE WANT THE TOP CRIME DESCRIPTION FOR EACH SQUARE
    //    val pw1 = new PrintWriter(new File("D:\\Eclipse Workspace\\LosAngelesCrime\\src\\FixedData\\crime_Codes.csv"))
    val pw1 = new PrintWriter(new File("resources/crime_Codes.csv"))
    crimeCode.collect().foreach({case ((long, lat), iter ) =>
      if(iter.head._2 > 1)
        pw1.write(long.toString + ", " + lat.toString + ", " + iter.head._1 + ": " + iter.head._2 + "\n")})
    pw1.close()

    // OPTION 2: IF WE WANT ALL CRIME DESCRIPTION FOR EACH GRID SQUARE
//    val pw1 = new PrintWriter(new File("resources/crime_Codes.csv"))
//    crimeCode.collect().foreach({case ((long, lat), (code, count)) =>
//      pw1.write(long.toString + ", " + lat.toString + ", " + code + ": " + count + "\n")})
//    pw1.close()


//    // numCodes has number of crimes in each (long, lat) pair
//    val numCrimes = crimeLines.map({case (coord, line) => coord})
//      // have (lat, long)
//      .map((_, 1))
//      // now ((lat, long), 1)
//      .reduceByKey(_+_)
//      // get number of crimes per grid square
//      .join(incomeLines)
//      .filter(_._2._1 > 100)
//      // filter any grid squares with < 100 crimes
//      .sortBy(_._2._2)
//      // sort by income
//
//    // val pw2 = new PrintWriter(new File("D:\\Eclipse Workspace\\LosAngelesCrime\\src\\FixedData\\crime_Counts.csv"))
//    val pw2 = new PrintWriter(new File("resources/crime_Counts.csv"))
//
//    numCrimes.collect.foreach({case ((long, lat), (count, income)) =>
//      pw2.write(long.toString + ", " + lat.toString + ":: Income: " + income + ", Count:" + count + "\n")
//      // pw2.write(income + "," + count + "\n")})   // FOR CSV FILE USAGE
//
//    })
//    pw2.close()


  }

  // splitting coords to more than 57 (if we want more)
  // method will round all the coords down to the lower 0.05
  def mutateCoords (x : Double): Double = {
    val rounded = (x * 100).round
    return (rounded - (rounded % 5)) / 100.0
  }
}