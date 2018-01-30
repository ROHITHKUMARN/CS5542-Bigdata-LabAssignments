import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by VenkatNag on 1/29/2018.
  */
object sparkprograming {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\UMKC\\Sum_May\\KDM\\winutils")
    val conf = new SparkConf().setAppName(s"Big data Lab").setMaster("local[*]").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.appName("Big data lab").master("local[*]").getOrCreate()
   import spark.implicits._
    val i = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").load("C:\\Users\\VenkatNag\\Desktop\\BigLab1\\ml-100k\\u.data")
     val names=Seq("UserId","ItemId","Rating","Timestamp")
    val table=i.toDF(names:_*)
    table.createOrReplaceTempView("ratingtable")
    val users=spark.sql("select UserId,count(ItemId) from ratingtable group by UserId having count(UserId) >25")
    users.show()

  }
}