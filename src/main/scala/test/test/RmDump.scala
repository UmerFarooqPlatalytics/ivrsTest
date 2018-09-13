package test.test

import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.DataFrame
import java.sql.Timestamp
import org.joda.time.format.DateTimeFormat
import java.math.MathContext
import play.api.libs.json.Json
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

object RmDump {

  val sparkSession = SparkSession.builder
    .master("local[*]").appName("test").getOrCreate

  def main(args: Array[String]) {
    //    dumpIntoRm

    var prop = new java.util.Properties
    val url = getConnectionString("S_NUMTRA", "S_NUMTRA#2018", "prd-db-scan.acurian.com", "1521", "acuprd_users.acurian.com")

//    val facilityQuery = s"""
//          SELECT * 
//          FROM  s_site.facility"""
////          WHERE ss1.study_id in ('148') AND 
////          ss1.facility_cd = ss2.facility_cd AND 
////          ss1.study_id = ss2.study_id AND 
////          ss2.site_num in ('216')"""
    
    
    val facilityQuery = s"""
          (SELECT ss1.facility_cd, ss1.site_num 
          FROM  s_site.study_site as ss1 , s_site.study_site as ss2
          WHERE ss1.study_id in ('148') AND 
          ss1.facility_cd = ss2.facility_cd AND 
          ss1.study_id = ss2.study_id AND 
          ss2.site_num in ('216'))
      """

    var facMap = scala.collection.immutable.Map.empty[String, String]
    sparkSession.sqlContext.read.jdbc(url, facilityQuery, prop).show/*.rdd.
      foreach(record => {
        if (record.getAs[String](1) != null && record.getAs[String](0) != null)
          facMap += (record.getAs[String](1) -> record.getAs[String](0))
      })*/
  }

  def dumpIntoRm() = {

    val mongoConn = new MongoDBConnector
    mongoConn.connect("172.16.248.23", "9876")
    val acurianStaging = mongoConn.getCollection("test", "acurianstagings")
    val dateFormat = "yyyy-MM-dd"

    val rddToWrite = acurianStaging.iterator.map(record => {

      val convertToFormat = (date: String, format: String) => {
        if (date != null)
          if (date.contains("T"))
            new Timestamp(DateTimeFormat.forPattern(format)
              .parseDateTime(date.split("T")(0))
              .getMillis)
          else
            new Timestamp(DateTimeFormat.forPattern(format)
              .parseDateTime(date.split(" ")(0))
              .getMillis)
        else
          null
      }

      val noneCheck = (value: Option[String]) => {
        if (value != None)
          value.get
        else
          null
      }

      val toDouble = (value: String) => {
        if (value != null) {
          println(value)
          new java.math.BigDecimal(value.replace("-", ""), MathContext.DECIMAL64)
        } else
          null
      }

      val jsonValue = Json.parse(record.toString)

      Row.fromSeq(Seq(noneCheck((jsonValue \ "IVRS_PROJECT_ID").asOpt[String]),
        noneCheck((jsonValue \ "IVRS_PROTOCOL_NUMBER").asOpt[String]),
        toDouble(noneCheck((jsonValue \ "IVRS_PATIENT_ID").asOpt[String])),
        noneCheck((jsonValue \ "IVRS_GENDER").asOpt[String]),
        noneCheck((jsonValue \ "IVRS_COUNTRY").asOpt[String]),
        noneCheck((jsonValue \ "IVRS_PATIENT_F_INITIAL").asOpt[String]),
        noneCheck((jsonValue \ "IVRS_PATIENT_M_INITIAL").asOpt[String]),
        noneCheck((jsonValue \ "IVRS_PATIENT_L_INITIAL").asOpt[String]),
        noneCheck((jsonValue \ "IVRS_REGION").asOpt[String]),
        noneCheck((jsonValue \ "IVRS_DOB_DAY").asOpt[String]),
        noneCheck((jsonValue \ "IVRS_DOB_MONTH").asOpt[String]),
        noneCheck((jsonValue \ "IVRS_DOB_YEAR").asOpt[String]),
        noneCheck((jsonValue \ "IVRS_SITE_ID").asOpt[String]),
        noneCheck((jsonValue \ "IVRS_INVESTIGATOR_F_INITIAL").asOpt[String]),
        noneCheck((jsonValue \ "IVRS_INVESTIGATOR_M_INITIAL").asOpt[String]),
        noneCheck((jsonValue \ "IVRS_INVESTIGATOR_L_INITIAL").asOpt[String]),
        convertToFormat(noneCheck((jsonValue \ "IVRS_DATE_SCREEN_FAILED").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "IVRS_DATE_PRE_SCREEN_FAILED").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "IVRS_DATE_DROPOUT").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "IVRS_DATE_PRE_SCREENED").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "IVRS_DATE_RANDOMIZATION_FAILED").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "IVRS_DATE_COMPLETED").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "IVRS_DATE_RE_SCREENED").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "IVRS_DATE_ENROLLMENT").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "IVRS_DATE_RANDOMIZED").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "IVRS_DATE_SCREENED").asOpt[String]), dateFormat),
        noneCheck((jsonValue \ "ACURIAN_PROJECT_ID").asOpt[String]),
        toDouble(noneCheck((jsonValue \ "ACURIAN_SSID").asOpt[String])),
        toDouble(noneCheck((jsonValue \ "ACURIAN_PATIENT_ID").asOpt[String])),
        noneCheck((jsonValue \ "ACURIAN_PROTOCOL_NUM").asOpt[String]),
        noneCheck((jsonValue \ "ACURIAN_SITE_ID").asOpt[String]),
        convertToFormat(noneCheck((jsonValue \ "ACURIAN_CONSENTED_DT").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "ACURIAN_RANDOMIZED_DT").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "ACURIAN_ENROLLED_DT").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "ACURIAN_RESOLVED_DT").asOpt[String]), dateFormat)))

    })

    val dataToWrite = sparkSession.sqlContext.applySchema(sparkSession.sparkContext.parallelize(rddToWrite.toList), outputSchema)

    dataToWrite.createOrReplaceTempView("table")
    var prop = new java.util.Properties
    val url = getConnectionString("S_NUMTRA", "numtradatasci#2018", "prd-db-scan.acurian.com", "1521", "acuprd_app_numtra.acurian.com")

    val dbc: Connection = DriverManager.getConnection(url)
    val st: PreparedStatement = dbc.prepareStatement("YOUR PREPARED STATEMENT")
    st.execute

    prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
    prop.setProperty("user", "S_NUMTRA")
    prop.setProperty("password", "numtradatasci#2018")
    prop.setProperty("allowExisting", "false")
    val outputTable = sparkSession.sqlContext.read.jdbc(url, "S_NUMTRA.IVRS_ACURIAN_OUTPUT", prop)
    outputTable.createOrReplaceTempView("outputTable")
    //outputTable.schema.fields.foreach(println)
    val rawData = sparkSession.sqlContext.sql("""
      select * from table where IVRS_PATIENT_ID IS NOT NULL
      """)

    //    rawData.show

    rawData.createOrReplaceTempView("newTable")
    val updateDF = sparkSession.sqlContext.sql("""select outputTable.* 
      from outputTable join newTable 
      on outputTable.IVRS_PROJECT_ID = newTable.IVRS_PROJECT_ID AND
      outputTable.IVRS_PROTOCOL_NUMBER = newTable.IVRS_PROTOCOL_NUMBER AND
      outputTable.IVRS_PATIENT_ID = newTable.IVRS_PATIENT_ID""")

    //updateDF.show

    outputTable.except(updateDF).createOrReplaceTempView("t1") //.drop("CREATE_DATE").drop("UPDATE_DATE").drop("MATCH_RANK").union(rawData).createOrReplaceTempView("oneMore")//union(rawData).
    val part1 = sparkSession.sqlContext.sql("""
      SELECT *
      FROM t1
      """)
    val part2 = sparkSession.sqlContext.sql("""
      SELECT *
      FROM newTable
      """)

    //    part1.show
    //    part2.show

    //outputTable.drop("CREATE_DATE").drop("UPDATE_DATE").drop("MATCH_RANK").show//.except(updateDF).show //.union(rawData).createOrReplaceTempView("forResult")
    //
    //    val result = sparkSession.sqlContext.sql("""
    //      SELECT *, NULL as MATCH_RANK
    //      FROM forResult
    //      WHERE IVRS_PATIENT_F_INITIAL = 'US'
    //      """)
    //
    //   result.show

    part1.write.mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url", url)
      .option("user", "S_NUMTRA")
      .option("password", "numtradatasci#2018")
      .option("dbtable", "S_NUMTRA.IVRS_ACURIAN_OUTPUT")
      .save()

    part2.write.mode(SaveMode.Append)
      .format("jdbc")
      .option("url", url)
      .option("user", "S_NUMTRA")
      .option("password", "numtradatasci#2018")
      .option("dbtable", "S_NUMTRA.IVRS_ACURIAN_OUTPUT")
      .save()

    //dataToWrite
  }

  def getConnectionString(userName: String, password: String, host: String, port: String, dbName: String): String = {
    s"jdbc:oracle:thin:${userName}/${password}@${host}:${port}/${dbName}"
    //s"jdbc:oracle:thin:${userName}/${password}@${host}:${port}/${dbName}"
  }

  def outputSchema(): StructType = {
    new StructType()
      .add(StructField("IVRS_PROJECT_ID", StringType, true))
      .add(StructField("IVRS_PROTOCOL_NUMBER", StringType, true))
      .add(StructField("IVRS_PATIENT_ID", DecimalType(38, 10), true))
      .add(StructField("IVRS_GENDER", StringType, true))
      .add(StructField("IVRS_COUNTRY", StringType, true))
      .add(StructField("IVRS_PATIENT_F_INITIAL", StringType, true))
      .add(StructField("IVRS_PATIENT_M_INITIAL", StringType, true))
      .add(StructField("IVRS_PATIENT_L_INITIAL", StringType, true))
      .add(StructField("IVRS_REGION", StringType, true))
      .add(StructField("IVRS_DOB_DAY", StringType, true))
      .add(StructField("IVRS_DOB_MONTH", StringType, true))
      .add(StructField("IVRS_DOB_YEAR", StringType, true))
      .add(StructField("IVRS_SITE_ID", StringType, true))
      .add(StructField("IVRS_INVESTIGATOR_F_INITIAL", StringType, true))
      .add(StructField("IVRS_INVESTIGATOR_M_INITIAL", StringType, true))
      .add(StructField("IVRS_INVESTIGATOR_L_INITIAL", StringType, true))
      .add(StructField("IVRS_DATE_SCREEN_FAILED", TimestampType, true))
      .add(StructField("IVRS_DATE_PRE_SCREEN_FAILED", TimestampType, true))
      .add(StructField("IVRS_DATE_DROPOUT", TimestampType, true))
      .add(StructField("IVRS_DATE_PRE_SCREENED", TimestampType, true))
      .add(StructField("IVRS_DATE_RANDOMIZATION_FAILED", TimestampType, true))
      .add(StructField("IVRS_DATE_COMPLETED", TimestampType, true))
      .add(StructField("IVRS_DATE_RE_SCREENED", TimestampType, true))
      .add(StructField("IVRS_DATE_ENROLLMENT", TimestampType, true))
      .add(StructField("IVRS_DATE_RANDOMIZED", TimestampType, true))
      .add(StructField("IVRS_DATE_SCREENED", TimestampType, true))
      .add(StructField("ACURIAN_PROJECT_ID", StringType, true))
      .add(StructField("ACURIAN_SSID", DecimalType(38, 10), true))
      .add(StructField("ACURIAN_PATIENT_ID", DecimalType(38, 10), true))
      .add(StructField("ACURIAN_PROTOCOL_NUM", StringType, true))
      .add(StructField("ACURIAN_SITE_ID", StringType, true))
      .add(StructField("ACURIAN_CONSENTED_DT", TimestampType, true))
      .add(StructField("ACURIAN_RANDOMIZED_DT", TimestampType, true))
      .add(StructField("ACURIAN_ENROLLED_DT", TimestampType, true))
      .add(StructField("ACURIAN_RESOLVED_DT", TimestampType, true))
  }

}