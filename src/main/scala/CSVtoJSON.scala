import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession


object CSVtoJSON {

  case class JsonObject(
                       col1: String,
                       col2: String,
                       col3: String,
                       col4: String,
                       col5: String,
                       col6: String,
                       col7: String,
                       col8: String,
                       col9: String,
                       col10: String,
                       col11: String
                       )

  def mapCsvSplitsToJsonColumns(line : String) : JsonObject ={
    val fields=line.split(',')

    val jsonObject : JsonObject=JsonObject(
      fields(0).toString,
      fields(1).toString,
      fields(2).toString,
      fields(3).toString,
      fields(4).toString,
      fields(5).toString,
      fields(6).toString,
      fields(7).toString,
      fields(8).toString,
      fields(9).toString,
      fields(10).toString)

    return jsonObject
  }


  def main(args: Array[String]): Unit = {



//    Set up spark session

    val spark = SparkSession.builder().appName("CSVtoJSON")
      .master("local[*]")
      .getOrCreate()

//    Load up each line
    val lines = spark.sparkContext.textFile("/Users/naveenkumar/IdeaProjects/gettingstarted/src/main/resources/PurchaseOrders.csv")

    val linesMapped = lines.map(x=>mapCsvSplitsToJsonColumns(x))

    import spark.implicits._

    val linesMappedDs = linesMapped.toDS()

    linesMappedDs.write.json("src/main/resources/JsonWrite")

    println("Process Complete")



  }


}
