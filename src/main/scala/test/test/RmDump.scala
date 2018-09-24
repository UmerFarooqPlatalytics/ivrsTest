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
    dumpIntoRm
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
          new java.math.BigDecimal(value.replace("-", ""), MathContext.DECIMAL64)
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
        convertToFormat(noneCheck((jsonValue \ "ACURIAN_CONSENTED_DT").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "ACURIAN_RANDOMIZED_DT").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "ACURIAN_ENROLLED_DT").asOpt[String]), dateFormat),
        convertToFormat(noneCheck((jsonValue \ "ACURIAN_RESOLVED_DT").asOpt[String]), dateFormat)))

    })

    val dataToWrite = sparkSession.sqlContext.applySchema(sparkSession.sparkContext.parallelize(rddToWrite.toList), outputSchema)

    dataToWrite.createOrReplaceTempView("table")
    var prop = new java.util.Properties
    val url = getConnectionString("S_NUMTRA", "S_NUMTRA#2018", "prd-db-scan.acurian.com", "1521", "acuprd_app_numtra.acurian.com")

    val dbc: Connection = DriverManager.getConnection(url)
    dbc.setAutoCommit(false)

    dataToWrite.rdd.collect.foreach(record => {

      val query = s"""
        
      MERGE INTO S_ACUTRACK.IVRS_ACURIAN_OUTPUT
      USING dual
      ON (IVRS_PROJECT_ID = '${record.getAs[String]("IVRS_PROJECT_ID")}' 
          AND IVRS_PROTOCOL_NUMBER = '${record.getAs[String]("IVRS_PROTOCOL_NUMBER")}' 
          AND IVRS_PATIENT_ID = '${record.getAs[String]("IVRS_PATIENT_ID")}' 
          
          
      WHEN MATCHED THEN
      UPDATE SET  IVRS_GENDER = ?,
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
                  ACURIAN_RESOLVED_DT = ?
       
      WHEN NOT MATCHED THEN   
      INSERT (IVRS_PROJECT_ID,IVRS_PROTOCOL_NUMBER,IVRS_PATIENT_ID,IVRS_GENDER,IVRS_COUNTRY,IVRS_PATIENT_F_INITIAL,IVRS_PATIENT_M_INITIAL,IVRS_PATIENT_L_INITIAL,IVRS_REGION,IVRS_DOB_DAY,IVRS_DOB_MONTH,IVRS_DOB_YEAR,IVRS_SITE_ID,IVRS_INVESTIGATOR_F_INITIAL,IVRS_INVESTIGATOR_M_INITIAL,IVRS_INVESTIGATOR_L_INITIAL,IVRS_DATE_SCREEN_FAILED,IVRS_DATE_PRE_SCREEN_FAILED,IVRS_DATE_DROPOUT,IVRS_DATE_PRE_SCREENED,IVRS_DATE_RANDOMIZATION_FAILED,IVRS_DATE_COMPLETED,IVRS_DATE_RE_SCREENED,IVRS_DATE_ENROLLMENT,IVRS_DATE_RANDOMIZED,IVRS_DATE_SCREENED,ACURIAN_PROJECT_ID,ACURIAN_SSID,ACURIAN_PATIENT_ID,ACURIAN_PROTOCOL_NUM,ACURIAN_SITE_ID,ACURIAN_CONSENTED_DT,ACURIAN_RANDOMIZED_DT,ACURIAN_ENROLLED_DT,ACURIAN_RESOLVED_DT)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """

      val st: PreparedStatement = dbc.prepareStatement(query)

      st.setString(1, record.getAs[String]("IVRS_GENDER"))
      st.setString(2, record.getAs[String]("IVRS_PATIENT_F_INITIAL"))
      st.setString(3, record.getAs[String]("IVRS_PATIENT_M_INITIAL"))
      st.setString(4, record.getAs[String]("IVRS_PATIENT_L_INITIAL"))
      st.setString(5, record.getAs[String]("IVRS_REGION"))
      st.setString(6, record.getAs[String]("IVRS_DOB_DAY"))
      st.setString(7, record.getAs[String]("IVRS_DOB_MONTH"))
      st.setString(8, record.getAs[String]("IVRS_DOB_YEAR"))
      st.setString(9, record.getAs[String]("IVRS_SITE_ID"))
      st.setString(10, record.getAs[String]("IVRS_INVESTIGATOR_F_INITIAL"))
      st.setString(11, record.getAs[String]("IVRS_INVESTIGATOR_M_INITIAL"))
      st.setString(12, record.getAs[String]("IVRS_INVESTIGATOR_L_INITIAL"))
      st.setTimestamp(13, record.getAs[Timestamp]("IVRS_DATE_SCREEN_FAILED"))
      st.setTimestamp(14, record.getAs[Timestamp]("IVRS_DATE_PRE_SCREEN_FAILED"))
      st.setTimestamp(15, record.getAs[Timestamp]("IVRS_DATE_DROPOUT"))
      st.setTimestamp(16, record.getAs[Timestamp]("IVRS_DATE_PRE_SCREENED"))
      st.setTimestamp(17, record.getAs[Timestamp]("IVRS_DATE_RANDOMIZATION_FAILED"))
      st.setTimestamp(18, record.getAs[Timestamp]("IVRS_DATE_COMPLETED"))
      st.setTimestamp(19, record.getAs[Timestamp]("IVRS_DATE_RE_SCREENED"))
      st.setTimestamp(20, record.getAs[Timestamp]("IVRS_DATE_ENROLLMENT"))
      st.setTimestamp(21, record.getAs[Timestamp]("IVRS_DATE_RANDOMIZED"))
      st.setTimestamp(22, record.getAs[Timestamp]("IVRS_DATE_SCREENED"))
      st.setString(23, record.getAs[String]("ACURIAN_PROJECT_ID"))
      st.setString(24, record.getAs[String]("ACURIAN_SSID"))
      st.setBigDecimal(25, record.getAs[java.math.BigDecimal]("ACURIAN_PATIENT_ID"))
      st.setString(26, record.getAs[String]("ACURIAN_PROTOCOL_NUM"))
      st.setString(27, record.getAs[String]("ACURIAN_SITE_ID"))
      st.setTimestamp(28, record.getAs[Timestamp]("ACURIAN_CONSENTED_DT"))
      st.setTimestamp(29, record.getAs[Timestamp]("ACURIAN_RANDOMIZED_DT"))
      st.setTimestamp(30, record.getAs[Timestamp]("ACURIAN_ENROLLED_DT"))
      st.setTimestamp(31, record.getAs[Timestamp]("ACURIAN_RESOLVED_DT"))
      st.setString(32, record.getAs[String]("IVRS_PROJECT_ID"))
      st.setString(33, record.getAs[String]("IVRS_PROTOCOL_NUMBER"))
      st.setString(34, record.getAs[String]("IVRS_PATIENT_ID"))
      st.setString(35, record.getAs[String]("IVRS_GENDER"))
      st.setString(36, record.getAs[String]("IVRS_COUNTRY"))
      st.setString(37, record.getAs[String]("IVRS_PATIENT_F_INITIAL"))
      st.setString(38, record.getAs[String]("IVRS_PATIENT_M_INITIAL"))
      st.setString(39, record.getAs[String]("IVRS_PATIENT_L_INITIAL"))
      st.setString(40, record.getAs[String]("IVRS_REGION"))
      st.setString(41, record.getAs[String]("IVRS_DOB_DAY"))
      st.setString(42, record.getAs[String]("IVRS_DOB_MONTH"))
      st.setString(43, record.getAs[String]("IVRS_DOB_YEAR"))
      st.setString(44, record.getAs[String]("IVRS_SITE_ID"))
      st.setString(45, record.getAs[String]("IVRS_INVESTIGATOR_F_INITIAL"))
      st.setString(46, record.getAs[String]("IVRS_INVESTIGATOR_M_INITIAL"))
      st.setString(47, record.getAs[String]("IVRS_INVESTIGATOR_L_INITIAL"))
      st.setTimestamp(48, record.getAs[Timestamp]("IVRS_DATE_SCREEN_FAILED"))
      st.setTimestamp(49, record.getAs[Timestamp]("IVRS_DATE_PRE_SCREEN_FAILED"))
      st.setTimestamp(50, record.getAs[Timestamp]("IVRS_DATE_DROPOUT"))
      st.setTimestamp(51, record.getAs[Timestamp]("IVRS_DATE_PRE_SCREENED"))
      st.setTimestamp(52, record.getAs[Timestamp]("IVRS_DATE_RANDOMIZATION_FAILED"))
      st.setTimestamp(53, record.getAs[Timestamp]("IVRS_DATE_COMPLETED"))
      st.setTimestamp(54, record.getAs[Timestamp]("IVRS_DATE_RE_SCREENED"))
      st.setTimestamp(55, record.getAs[Timestamp]("IVRS_DATE_ENROLLMENT"))
      st.setTimestamp(56, record.getAs[Timestamp]("IVRS_DATE_RANDOMIZED"))
      st.setTimestamp(57, record.getAs[Timestamp]("IVRS_DATE_SCREENED"))
      st.setString(58, record.getAs[String]("ACURIAN_PROJECT_ID"))
      st.setString(59, record.getAs[String]("ACURIAN_SSID"))
      st.setBigDecimal(60, record.getAs[java.math.BigDecimal]("ACURIAN_PATIENT_ID"))
      st.setString(61, record.getAs[String]("ACURIAN_PROTOCOL_NUM"))
      st.setString(62, record.getAs[String]("ACURIAN_SITE_ID"))
      st.setTimestamp(63, record.getAs[Timestamp]("ACURIAN_CONSENTED_DT"))
      st.setTimestamp(64, record.getAs[Timestamp]("ACURIAN_RANDOMIZED_DT"))
      st.setTimestamp(65, record.getAs[Timestamp]("ACURIAN_ENROLLED_DT"))
      st.setTimestamp(66, record.getAs[Timestamp]("ACURIAN_RESOLVED_DT"))

      st.execute
      st.close
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
      .add(StructField("ACURIAN_CONSENTED_DT", TimestampType, true))
      .add(StructField("ACURIAN_RANDOMIZED_DT", TimestampType, true))
      .add(StructField("ACURIAN_ENROLLED_DT", TimestampType, true))
      .add(StructField("ACURIAN_RESOLVED_DT", TimestampType, true))
  }

}