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
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import com.mongodb.DBObject

object RmDump {

  val sparkSession = SparkSession.builder
    .master("local[*]").appName("test").getOrCreate

  def main(args: Array[String]) {
    dumpIntoRm
  }

  def dumpIntoRm() = {

    val mongoConn = new MongoDBConnector
    mongoConn.connect("ds-dev-node-04", "9876")
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

      val noneCheckForInt = (value: Option[Int]) => {
        if (value != None)
          int2Integer(value.get)
        else
          null
      }

      val toDouble = (value: String) => {
        if (value != null) {
          new java.math.BigDecimal(value.replace("-", ""), MathContext.DECIMAL64)
        } else
          null
      }

      val intToDouble = (value: Integer) => {
        if (value != null) {
          new java.math.BigDecimal(value, MathContext.DECIMAL64)
        } else
          null
      }

      val jsonValue = Json.parse(record.toString)

      Row.fromSeq(Seq(noneCheck((jsonValue \ "IVRS_PROJECT_ID").asOpt[String]),
        noneCheck((jsonValue \ "IVRS_PROTOCOL_NUMBER").asOpt[String]),
        noneCheck((jsonValue \ "IVRS_PATIENT_ID").asOpt[String]),
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
        noneCheck((jsonValue \ "ACURIAN_SSID").asOpt[String]),
        toDouble(noneCheck((jsonValue \ "ACURIAN_PATIENT_ID").asOpt[String])),
        noneCheck((jsonValue \ "ACURIAN_PROTOCOL_NUM").asOpt[String]),
        noneCheck((jsonValue \ "ACURIAN_SITE_ID").asOpt[String]),
        intToDouble(noneCheckForInt((jsonValue \ "SYSTEM_RANK").asOpt[Int])),
        toDouble(noneCheck((jsonValue \ "CONFIRMATION_METHOD_CD").asOpt[String])),
        convertToFormat(noneCheck((jsonValue \ "ACURIAN_CONSENTED_DT").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "ACURIAN_RANDOMIZED_DT").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "ACURIAN_ENROLLED_DT").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "ACURIAN_RESOLVED_DT").asOpt[String]), dateFormat)))

    })

    val dataToWrite = sparkSession.sqlContext.applySchema(sparkSession.sparkContext.parallelize(rddToWrite.toList), outputSchema)

    dataToWrite.createOrReplaceTempView("table")
    var prop = new java.util.Properties
    //val url = getConnectionString("S_NUMTRA", "S_NUMTRA#2018", "prd-db-scan.acurian.com", "1521", "acuprd_app_numtra.acurian.com")
    val url = getConnectionString("S_NUMTRA", "S_NUMTRA#2018", "dev-db-scan.acurian.com", "1521", "acuqa_users.acurian.com")

    val dbc: Connection = DriverManager.getConnection(url)
    dbc.setAutoCommit(true)

    val dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    val now = LocalDateTime.now()
    val curDate = dtf.format(now)
    val updateSummary = sparkSession.sqlContext.sql(s"""
      SELECT IVRS_PROJECT_ID,
             IVRS_COUNTRY,
             IVRS_PROTOCOL_NUMBER,
             SUM (CASE SYSTEM_RANK WHEN 0 THEN 1 ELSE 0 END) as TOTAL_EXACT_MATCHES,
             SUM (CASE SYSTEM_RANK WHEN SYSTEM_RANK > 0 THEN 1 ELSE 0 END) as TOTAL_CONFIRMED_MATCHES,
             '${curDate}' as DATE_UPDATED
      FROM (SELECT IVRS_PROJECT_ID,
             IVRS_COUNTRY,
             IVRS_PROTOCOL_NUMBER,
             CAST(FLOOR(SYSTEM_RANK) AS INT) as SYSTEM_RANK
             FROM table)
      GROUP BY IVRS_PROJECT_ID, IVRS_COUNTRY, IVRS_PROTOCOL_NUMBER      
      """)

    sparkSession.sqlContext.sql(s"""
      SELECT IVRS_PROJECT_ID,
             IVRS_COUNTRY,
             IVRS_PROTOCOL_NUMBER,
             CAST(FLOOR(SYSTEM_RANK) AS INT) as SYSTEM_RANK
      FROM table
      WHERE SYSTEM_RANK > 0
      """).show

    sparkSession.sqlContext.sql(s"""
      SELECT IVRS_PROJECT_ID,
             IVRS_COUNTRY,
             IVRS_PROTOCOL_NUMBER,
             CAST(FLOOR(SYSTEM_RANK) AS INT) as SYSTEM_RANK
      FROM table
      """).show(353)
      
      sparkSession.sqlContext.sql(s"""
      SELECT IVRS_PROJECT_ID,
             IVRS_COUNTRY,
             IVRS_PROTOCOL_NUMBER,
             SYSTEM_RANK
      FROM table
      """).show(353)

    dumpForDashboard(updateSummary, "acurianupdatesummaries")

    dataToWrite.rdd.collect.foreach(record => {

      val updateQuery = s"""
      
    UPDATE S_ACUTRACK.IVRS_ACURIAN_OUTPUT SET
                  IVRS_GENDER = ?,
                  IVRS_PATIENT_F_INITIAL = ?,
                  IVRS_PATIENT_M_INITIAL = ?,
                  IVRS_PATIENT_L_INITIAL = ?,
                  IVRS_REGION = ?,
                  IVRS_DOB_DAY = ?,
                  IVRS_DOB_MONTH = ?,
                  IVRS_DOB_YEAR = ?,
                  IVRS_SITE_ID = ?,
                  IVRS_INVESTIGATOR_F_INITIAL = ?,
                  IVRS_INVESTIGATOR_M_INITIAL = ?,
                  IVRS_INVESTIGATOR_L_INITIAL = ?,
                  IVRS_DATE_SCREEN_FAILED = ?,
                  IVRS_DATE_PRE_SCREEN_FAILED = ?,
                  IVRS_DATE_DROPOUT = ?,
                  IVRS_DATE_PRE_SCREENED = ?,
                  IVRS_DATE_RANDOMIZATION_FAILED = ?,
                  IVRS_DATE_COMPLETED = ?,
                  IVRS_DATE_RE_SCREENED = ?,
                  IVRS_DATE_ENROLLMENT = ?,
                  IVRS_DATE_RANDOMIZED = ?,
                  IVRS_DATE_SCREENED = ?,
                  ACURIAN_PROJECT_ID = ?,
                  ACURIAN_SSID = ?,
                  ACURIAN_PATIENT_ID = ?,
                  ACURIAN_PROTOCOL_NUM = ?,
                  ACURIAN_SITE_ID = ?,
                  ACURIAN_CONSENTED_DT = ?,
                  ACURIAN_RANDOMIZED_DT = ?,
                  ACURIAN_ENROLLED_DT = ?,
                  ACURIAN_RESOLVED_DT = ?,
                  MATCH_RANK = ?,
                  CONFIRMATION_METHOD_CD = ?
    WHERE '${record.getAs[String]("IVRS_PROJECT_ID")}' = IVRS_PROJECT_ID 
      AND '${record.getAs[String]("IVRS_PROTOCOL_NUMBER")}' = IVRS_PROTOCOL_NUMBER
      AND '${record.getAs[String]("IVRS_PATIENT_ID")}' = IVRS_PATIENT_ID 
      AND '${record.getAs[String]("IVRS_COUNTRY")}' = IVRS_COUNTRY"""

      val insertQuery = """
      INSERT INTO S_ACUTRACK.IVRS_ACURIAN_OUTPUT (IVRS_PROJECT_ID, IVRS_PROTOCOL_NUMBER, IVRS_PATIENT_ID, IVRS_GENDER, IVRS_COUNTRY, IVRS_PATIENT_F_INITIAL, IVRS_PATIENT_M_INITIAL, IVRS_PATIENT_L_INITIAL, IVRS_REGION, IVRS_DOB_DAY, IVRS_DOB_MONTH, IVRS_DOB_YEAR, IVRS_SITE_ID, IVRS_INVESTIGATOR_F_INITIAL, IVRS_INVESTIGATOR_M_INITIAL, IVRS_INVESTIGATOR_L_INITIAL, IVRS_DATE_SCREEN_FAILED, IVRS_DATE_PRE_SCREEN_FAILED, IVRS_DATE_DROPOUT, IVRS_DATE_PRE_SCREENED, IVRS_DATE_RANDOMIZATION_FAILED, IVRS_DATE_COMPLETED, IVRS_DATE_RE_SCREENED, IVRS_DATE_ENROLLMENT, IVRS_DATE_RANDOMIZED, IVRS_DATE_SCREENED, ACURIAN_PROJECT_ID, ACURIAN_SSID, ACURIAN_PATIENT_ID, ACURIAN_PROTOCOL_NUM, ACURIAN_SITE_ID, ACURIAN_CONSENTED_DT, ACURIAN_RANDOMIZED_DT, ACURIAN_ENROLLED_DT, ACURIAN_RESOLVED_DT, MATCH_RANK, CONFIRMATION_METHOD_CD)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

      val fetchQuery = s"""
      SELECT * 
      FROM S_ACUTRACK.IVRS_ACURIAN_OUTPUT
      WHERE '${record.getAs[String]("IVRS_PROJECT_ID")}' = IVRS_PROJECT_ID 
      AND '${record.getAs[String]("IVRS_PROTOCOL_NUMBER")}' = IVRS_PROTOCOL_NUMBER
      AND '${record.getAs[String]("IVRS_PATIENT_ID")}' = IVRS_PATIENT_ID 
      """

      val updateStatement: PreparedStatement = dbc.prepareStatement(updateQuery)
      val insertStatement: PreparedStatement = dbc.prepareStatement(insertQuery)

      updateStatement.setString(1, record.getAs[String]("IVRS_GENDER"))
      updateStatement.setString(2, record.getAs[String]("IVRS_PATIENT_F_INITIAL"))
      updateStatement.setString(3, record.getAs[String]("IVRS_PATIENT_M_INITIAL"))
      updateStatement.setString(4, record.getAs[String]("IVRS_PATIENT_L_INITIAL"))
      updateStatement.setString(5, record.getAs[String]("IVRS_REGION"))
      updateStatement.setString(6, record.getAs[String]("IVRS_DOB_DAY"))
      updateStatement.setString(7, record.getAs[String]("IVRS_DOB_MONTH"))
      updateStatement.setString(8, record.getAs[String]("IVRS_DOB_YEAR"))
      updateStatement.setString(9, record.getAs[String]("IVRS_SITE_ID"))
      updateStatement.setString(10, record.getAs[String]("IVRS_INVESTIGATOR_F_INITIAL"))
      updateStatement.setString(11, record.getAs[String]("IVRS_INVESTIGATOR_M_INITIAL"))
      updateStatement.setString(12, record.getAs[String]("IVRS_INVESTIGATOR_L_INITIAL"))
      updateStatement.setTimestamp(13, record.getAs[Timestamp]("IVRS_DATE_SCREEN_FAILED"))
      updateStatement.setTimestamp(14, record.getAs[Timestamp]("IVRS_DATE_PRE_SCREEN_FAILED"))
      updateStatement.setTimestamp(15, record.getAs[Timestamp]("IVRS_DATE_DROPOUT"))
      updateStatement.setTimestamp(16, record.getAs[Timestamp]("IVRS_DATE_PRE_SCREENED"))
      updateStatement.setTimestamp(17, record.getAs[Timestamp]("IVRS_DATE_RANDOMIZATION_FAILED"))
      updateStatement.setTimestamp(18, record.getAs[Timestamp]("IVRS_DATE_COMPLETED"))
      updateStatement.setTimestamp(19, record.getAs[Timestamp]("IVRS_DATE_RE_SCREENED"))
      updateStatement.setTimestamp(20, record.getAs[Timestamp]("IVRS_DATE_ENROLLMENT"))
      updateStatement.setTimestamp(21, record.getAs[Timestamp]("IVRS_DATE_RANDOMIZED"))
      updateStatement.setTimestamp(22, record.getAs[Timestamp]("IVRS_DATE_SCREENED"))
      updateStatement.setString(23, record.getAs[String]("ACURIAN_PROJECT_ID"))
      updateStatement.setString(24, record.getAs[String]("ACURIAN_SSID"))
      updateStatement.setBigDecimal(25, record.getAs[java.math.BigDecimal]("ACURIAN_PATIENT_ID"))
      updateStatement.setString(26, record.getAs[String]("ACURIAN_PROTOCOL_NUM"))
      updateStatement.setString(27, record.getAs[String]("ACURIAN_SITE_ID"))
      updateStatement.setTimestamp(28, record.getAs[Timestamp]("ACURIAN_CONSENTED_DT"))
      updateStatement.setTimestamp(29, record.getAs[Timestamp]("ACURIAN_RANDOMIZED_DT"))
      updateStatement.setTimestamp(30, record.getAs[Timestamp]("ACURIAN_ENROLLED_DT"))
      updateStatement.setTimestamp(31, record.getAs[Timestamp]("ACURIAN_RESOLVED_DT"))
      updateStatement.setBigDecimal(32, record.getAs[java.math.BigDecimal]("SYSTEM_RANK"))
      updateStatement.setBigDecimal(33, record.getAs[java.math.BigDecimal]("CONFIRMATION_METHOD_CD"))
      insertStatement.setString(1, record.getAs[String]("IVRS_PROJECT_ID"))
      insertStatement.setString(2, record.getAs[String]("IVRS_PROTOCOL_NUMBER"))
      insertStatement.setString(3, record.getAs[String]("IVRS_PATIENT_ID"))
      insertStatement.setString(4, record.getAs[String]("IVRS_GENDER"))
      insertStatement.setString(5, record.getAs[String]("IVRS_COUNTRY"))
      insertStatement.setString(6, record.getAs[String]("IVRS_PATIENT_F_INITIAL"))
      insertStatement.setString(7, record.getAs[String]("IVRS_PATIENT_M_INITIAL"))
      insertStatement.setString(8, record.getAs[String]("IVRS_PATIENT_L_INITIAL"))
      insertStatement.setString(9, record.getAs[String]("IVRS_REGION"))
      insertStatement.setString(10, record.getAs[String]("IVRS_DOB_DAY"))
      insertStatement.setString(11, record.getAs[String]("IVRS_DOB_MONTH"))
      insertStatement.setString(12, record.getAs[String]("IVRS_DOB_YEAR"))
      insertStatement.setString(13, record.getAs[String]("IVRS_SITE_ID"))
      insertStatement.setString(14, record.getAs[String]("IVRS_INVESTIGATOR_F_INITIAL"))
      insertStatement.setString(15, record.getAs[String]("IVRS_INVESTIGATOR_M_INITIAL"))
      insertStatement.setString(16, record.getAs[String]("IVRS_INVESTIGATOR_L_INITIAL"))
      insertStatement.setTimestamp(17, record.getAs[Timestamp]("IVRS_DATE_SCREEN_FAILED"))
      insertStatement.setTimestamp(18, record.getAs[Timestamp]("IVRS_DATE_PRE_SCREEN_FAILED"))
      insertStatement.setTimestamp(19, record.getAs[Timestamp]("IVRS_DATE_DROPOUT"))
      insertStatement.setTimestamp(20, record.getAs[Timestamp]("IVRS_DATE_PRE_SCREENED"))
      insertStatement.setTimestamp(21, record.getAs[Timestamp]("IVRS_DATE_RANDOMIZATION_FAILED"))
      insertStatement.setTimestamp(22, record.getAs[Timestamp]("IVRS_DATE_COMPLETED"))
      insertStatement.setTimestamp(23, record.getAs[Timestamp]("IVRS_DATE_RE_SCREENED"))
      insertStatement.setTimestamp(24, record.getAs[Timestamp]("IVRS_DATE_ENROLLMENT"))
      insertStatement.setTimestamp(25, record.getAs[Timestamp]("IVRS_DATE_RANDOMIZED"))
      insertStatement.setTimestamp(26, record.getAs[Timestamp]("IVRS_DATE_SCREENED"))
      insertStatement.setString(27, record.getAs[String]("ACURIAN_PROJECT_ID"))
      insertStatement.setString(28, record.getAs[String]("ACURIAN_SSID"))
      insertStatement.setBigDecimal(29, record.getAs[java.math.BigDecimal]("ACURIAN_PATIENT_ID"))
      insertStatement.setString(30, record.getAs[String]("ACURIAN_PROTOCOL_NUM"))
      insertStatement.setString(31, record.getAs[String]("ACURIAN_SITE_ID"))
      insertStatement.setTimestamp(32, record.getAs[Timestamp]("ACURIAN_CONSENTED_DT"))
      insertStatement.setTimestamp(33, record.getAs[Timestamp]("ACURIAN_RANDOMIZED_DT"))
      insertStatement.setTimestamp(34, record.getAs[Timestamp]("ACURIAN_ENROLLED_DT"))
      insertStatement.setTimestamp(35, record.getAs[Timestamp]("ACURIAN_RESOLVED_DT"))
      insertStatement.setBigDecimal(36, record.getAs[java.math.BigDecimal]("SYSTEM_RANK"))
      insertStatement.setBigDecimal(37, record.getAs[java.math.BigDecimal]("CONFIRMATION_METHOD_CD"))

//      updateStatement.execute
//      updateStatement.close
//      val forInsert = sparkSession.sqlContext.read.jdbc(url, s"(${fetchQuery})", prop)
//
//      if (forInsert.count == 0 && record.getAs[String]("IVRS_PROJECT_ID") != null && record.getAs[String]("IVRS_PROTOCOL_NUMBER") != null
//        && record.getAs[String]("IVRS_PATIENT_ID") != null && record.getAs[String]("IVRS_COUNTRY") != null) {
//        insertStatement.execute
//        insertStatement.close
//      }
    })

    dbc.close
  }

  def getConnectionString(userName: String, password: String, host: String, port: String, dbName: String): String = {
    s"jdbc:oracle:thin:${userName}/${password}@${host}:${port}/${dbName}"
  }

  def outputSchema(): StructType = {
    new StructType()
      .add(StructField("IVRS_PROJECT_ID", StringType, true))
      .add(StructField("IVRS_PROTOCOL_NUMBER", StringType, true))
      .add(StructField("IVRS_PATIENT_ID", StringType, true))
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
      .add(StructField("ACURIAN_SSID", StringType, true))
      .add(StructField("ACURIAN_PATIENT_ID", DecimalType(38, 10), true))
      .add(StructField("ACURIAN_PROTOCOL_NUM", StringType, true))
      .add(StructField("ACURIAN_SITE_ID", StringType, true))
      .add(StructField("SYSTEM_RANK", DecimalType(38, 10), true))
      .add(StructField("CONFIRMATION_METHOD_CD", DecimalType(38, 10), true))
      .add(StructField("ACURIAN_CONSENTED_DT", TimestampType, true))
      .add(StructField("ACURIAN_RANDOMIZED_DT", TimestampType, true))
      .add(StructField("ACURIAN_ENROLLED_DT", TimestampType, true))
      .add(StructField("ACURIAN_RESOLVED_DT", TimestampType, true))
  }

  def dumpForDashboard(data: DataFrame, collectionName: String) = {
    data.toJSON.rdd.foreachPartition(partition => {
      import sparkSession.implicits._
      val mongoConn = new MongoDBConnector
      mongoConn.connect("ds-dev-node-04", "9876")
      val dataBase: com.mongodb.casbah.MongoDB = mongoConn.mongoClient("test")
      val collection = dataBase(collectionName)
      partition.foreach(jsonRow => {
        val jsonRecord = com.mongodb.util.JSON.parse(jsonRow).asInstanceOf[DBObject]
        collection.insert(jsonRecord)
      })
    })
  }

}