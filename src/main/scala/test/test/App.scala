package test.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO
import org.zuinnote.hadoop.office.format.mapreduce.ExcelFileInputFormat
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.Text
import java.io.DataInput
import play.api.libs.json.Json
import java.util.Formatter.DateTime
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.sql.types.DecimalType

/**
 * @author ${user.name}
 */
object App {
  val sparkSession = SparkSession.builder
    .master("local[*]").appName("test").getOrCreate
  //  var dinput: DataInput = _
  def main(args: Array[String]) {
    
    sparkSession.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "false")
      .option("header", "true")
      .option("delimiter", ",")
      .load("file:///C:/Users/umer/Downloads/3017-M16_100-US-20180828.csv").show

//    val mongoCon = new MongoDBConnector
//    mongoCon.connect("172.16.248.23", "9876")
//    val project = Json.parse(mongoCon.getRecordsByKey("test", "projects", "project_id", "4579"))
//    val projectStartDate = (project \ "project_start_date") //.as[DateTime]
//    //val ivrsRunStartDate = (project \ "ivrs_run_start_date").as[String]
//
//    //val t = mongoCon.getCollection(null, "acurianstagings")
//    //println(t.size)
//    println(projectStartDate.get.toString.split("T")(0).drop(10))
//
//    //    val conf: Configuration = new Configuration()
//    //    conf.set("fs.defaultFS", "ds-node1:9000")
//    //
//    //    var filePath = s"/user/plat/files/ivrsData/4579-JZP166_201-US-20180719.xlsx"
//    //    val rdd = convertToRdd(conf, "hdfs://ds-node1.acurian.com:9000" + filePath, 0)
//    //    println(rdd.count)
//
//    val t = Seq(Row.fromSeq(Seq("JZP166_201", "4579", java.math.BigDecimal.valueOf(123.0), "US")))
//    //
//    val data = sparkSession.sparkContext.parallelize(t)
//    val schema = new StructType()
//      .add(StructField("ivrs_protocol_number", StringType, true))
//      .add(StructField("ivrs_project_id", StringType, true))
//      .add(StructField("ivrs_patient_id", DecimalType(38, 10), true))
//      .add(StructField("ivrs_country", StringType, true))
//    val dataFrame = sparkSession.sqlContext.applySchema(data, schema)
//
//    var i = 0
//
//   val h = sparkSession.sparkContext.broadcast(dataFrame.rdd.collect)
//   
//   val j = dataFrame.rdd.flatMap(r => {
//     h.value
//   }).collect.foreach(println)
//   

    //    dataFrame.rdd.foreach ( row => {
    //      println(row.getAs[java.math.BigDecimal]("ivrs_patient_id").toString.split('.')(0))
    //    })

    /*val u = dataFrame.rdd.mapPartitions(partition => {
      import sparkSession.implicits._
      val mongoConn = new MongoDBConnector
      mongoConn.connect("172.16.248.23", "9876")
      val dataBase: com.mongodb.casbah.MongoDB = mongoConn.mongoClient("test")
      val rejectedCollection = dataBase("ivrsrejectedrecords")
      val inprocessCollection = dataBase("dashboardfilerecords")

      val newPartition = partition.filter(row => {

        val inProcessRecords = MongoDBObject("ivrs_protocol_number" -> row.getAs[String]("ivrs_protocol_number"),
          "ivrs_project_id" -> row.getAs[String]("ivrs_project_id"),
          "ivrs_patient_id" -> row.getAs[String]("ivrs_patient_id"),
          "ivrs_country" -> row.getAs[String]("ivrs_country"))

        val inProcessResult = inprocessCollection.findOne(inProcessRecords)
        println("status ================= " + (Json.parse(inProcessResult.getOrElse().toString) \ "status").as[String])

        if (inProcessResult != None) {
          if ((Json.parse(inProcessResult.getOrElse().toString) \ "status").as[String].equals("In Process")) {
            println("In Process Found")
            false
          } else
            false
        } else {
          true
        }
      })
      newPartition
    })

    u.collect.foreach(println)*/

    //
    //    val dataFrame = sparkSession.sqlContext.applySchema(data, schema)  
    //    
    //    dataFrame.rdd.foreachPartition(partition => {
    //      import sparkSession.implicits._
    //      val mongoConn = new MongoDBConnector
    //      mongoConn.connect("172.16.248.23", "9876")
    //      val dataBase: com.mongodb.casbah.MongoDB = mongoConn.mongoClient("test")
    //      val collection = dataBase("test")
    //
    //      partition.foreach(jsonRow => {
    //        val rowMap = jsonRow.getValuesMap[Any](jsonRow.schema.fieldNames)
    //        mongoConn.updateAcurianStagingRecord("test", "testUpSert", rowMap, 
    //            jsonRow.getAs[String]("id"),jsonRow.getAs[String]("idd"))
    //      })
    //    })
    //    
    //    
    //    

    //    
    //    dataFrame.createOrReplaceTempView("table")
    //    
    //    sparkSession.sql("""SELECT table.* 
    //      FROM table
    //      JOIN(SELECT ID, MIN(RANK) as mrank
    //      FROM table
    //      GROUP BY ID
    //      ) as minrank on table.id = minrank.id and table.rank = minrank.mrank """).show

  }

  //  def convertToRdd(hadoopConf: Configuration, inputFile: String, headerLines: Int): RDD[String] = {
  //    // load using the new Hadoop API (mapreduce.*)
  //
  //    val excelRDD = sparkSession.sparkContext.newAPIHadoopFile(inputFile, classOf[ExcelFileInputFormat],
  //      classOf[Text], classOf[ArrayWritable], hadoopConf)
  //    // print the cell address and the formatted cell content
  //    excelRDD.map(hadoopKeyValueTuple => {
  //      // note see also org.zuinnote.hadoop.office.format.common.converter.ExcelConverterSimpleSpreadSheetCellDAO to convert from and to SpreadSheetCellDAO
  //      val rowStrBuffer = new StringBuilder
  //      var i = 0;
  //      for (x <- hadoopKeyValueTuple._2.get) { // parse through the SpreadSheetCellDAO
  //        if (x != null) {
  //          val t = x.asInstanceOf[SpreadSheetCellDAO].formatted("MM/dd/yyyy")
  //          println(t)
  //          println(x.asInstanceOf[SpreadSheetCellDAO].getFormattedValue)
  //          rowStrBuffer.append(s""""${x.asInstanceOf[SpreadSheetCellDAO].getFormattedValue}",""")
  //        } else {
  //          rowStrBuffer.append(",")
  //        }
  //        i += 1
  //      }
  //      if (rowStrBuffer.length > 0) {
  //        rowStrBuffer.deleteCharAt(rowStrBuffer.length - 1) // remove last comma
  //      }
  //      rowStrBuffer.toString
  //    }).mapPartitionsWithIndex(
  //      (index, it) => if (index == 0) it.drop(headerLines) else it,
  //      preservesPartitioning = true)
  //  }

}
