package test.test

import java.sql.Timestamp
import java.util.Locale

import scala.annotation.implicitNotFound
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.joda.time.format.DateTimeFormat
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO
import org.zuinnote.hadoop.office.format.mapreduce.ExcelFileInputFormat

import com.mongodb.DBObject
import com.mongodb.casbah.commons.MongoDBObject
import play.api.libs.json.JsValue
import play.api.libs.json.JsValue.jsValueToJsLookup
import play.api.libs.json.Json
import java.util.Calendar
import scala.util.parsing.json.JSONObject
import org.apache.spark.storage.StorageLevel
import java.math.MathContext
import org.apache.spark.sql.SparkSession

object RmSpike {

  val sparkSession = SparkSession.builder
    .master("local[*]").appName("test").getOrCreate

  val sql = sparkSession.sqlContext
  val mongoCon = new MongoDBConnector
  mongoCon.connect(Constants.PROCESS_MONGO_IP, Constants.PROCESS_MONGO_PORT)
  //val hdfsPath = "hdfs://ds-node1:9000"
  //val hdfsPath = "hdfs://ds-node6:9000"
  val hdfsPath = "hdfs://ds-dev-node-01:9000"

  def main(args: Array[String]) {
    run("3889-M14_431-US-20181105.csv", 1)
  }

  def run(ivrsFileName: String, headerLines: Int): DataFrame = {

    val acurianData = sql.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ",")
      .load(s"${hdfsPath}/RmData")

    var filePath = Constants.IVRS_FILES_PATH + s"/${ivrsFileName}"

    val ivrsData = loadIvrsData(ivrsFileName, headerLines - 1)

    val projectID = ivrsFileName.split("-")(0)
    val project = mongoCon.getRecordsByKey("test", "projects", "project_id", projectID)
    val rankThreshold = (Json.parse(project) \ "rank_threshold").as[Int]

    val schemaAppliedData = mapSchema(ivrsFileName, ivrsData) //.cache

    //dumpForDashboard(acurianData, "acurianData")
    schemaAppliedData.createOrReplaceTempView("schemaAppliedData")

    val tempDataForStagingTable = sql.sql("""
      SELECT  ivrs_patient_id as IVRS_PATIENT_ID,
	            dob_day as IVRS_DOB_DAY,
	            dob_month as IVRS_DOB_MONTH,
	            dob_year as IVRS_DOB_YEAR,
	            patient_f_initial as IVRS_PATIENT_F_INITIAL,
	            patient_m_initial as IVRS_PATIENT_M_INITIAL,
	            patient_l_initial as IVRS_PATIENT_L_INITIAL,
	            investigator_f_name as IVRS_INVESTIGATOR_F_INITIAL,
	            investigator_m_name as IVRS_INVESTIGATOR_M_INITIAL,
	            investigator_l_name as IVRS_INVESTIGATOR_L_INITIAL,
	            gender as IVRS_GENDER,
	            date_screened as IVRS_DATE_SCREENED,
	            date_screen_failed as IVRS_DATE_SCREEN_FAILED,
	            date_randomized as IVRS_DATE_RANDOMIZED,
	            date_completed as IVRS_DATE_COMPLETED,
	            date_re_screened as IVRS_DATE_RE_SCREENED,
	            date_pre_screened as IVRS_DATE_PRE_SCREENED,
	            date_randomization_failed as IVRS_DATE_RANDOMIZATION_FAILED,
	            date_pre_screen_failed as IVRS_DATE_PRE_SCREEN_FAILED,
	            date_enrollment as IVRS_DATE_ENROLLMENT,
	            date_dropout as IVRS_DATE_DROPOUT,
	            region as IVRS_REGION,
	            country as IVRS_COUNTRY,
	            protocol_number as IVRS_PROTOCOL_NUMBER,
	            site_id as IVRS_SITE_ID,
	            project_id as IVRS_PROJECT_ID
	    FROM schemaAppliedData
      """)

    updateInStaging(tempDataForStagingTable)

    val rawRanks = applyRanks(ivrsFileName, schemaAppliedData, acurianData) //.filter(ranksApplied("system_rank") < 5000).persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView("rankedData")
    val matches = rawRanks.filter(rawRanks("system_rank") < 5000).persist(StorageLevel.MEMORY_AND_DISK_SER)
    matches.show
    matches.createOrReplaceTempView("rankedData")

//    matches.rdd.map(matched => {
//
//    })

    val exactMatches = sql.sql(s"""
         SELECT acurian_screening_id as ACURIAN_SSID,
                acurian_project_id as ACURIAN_PROJECT_ID, 
                ivrs_project_id as IVRS_PROJECT_ID, 
                acurian_consented_protocol as ACURIAN_PROTOCOL_NUM, 
                ivrs_protocol_number as IVRS_PROTOCOL_NUMBER, 
                ivrs_country as IVRS_COUNTRY,
                acurian_patient_id as ACURIAN_PATIENT_ID, 
                ivrs_patient_id as IVRS_PATIENT_ID,
                ivrs_dob_d as IVRS_DOB_DAY, 
                ivrs_dob_m as IVRS_DOB_MONTH,
                ivrs_dob_y as IVRS_DOB_YEAR, 
                ivrs_gender as IVRS_GENDER, 
                ivrs_pt_fi as IVRS_PATIENT_F_INITIAL, 
                ivrs_pt_mi as IVRS_PATIENT_M_INITIAL, 
                ivrs_pt_li as IVRS_PATIENT_L_INITIAL, 
                ivrs_invst_f_name as IVRS_INVESTIGATOR_F_INITIAL,
                ivrs_invst_m_name as IVRS_INVESTIGATOR_M_INITIAL, 
                ivrs_invst_l_name as IVRS_INVESTIGATOR_L_INITIAL,
                ivrs_date_screened as IVRS_DATE_SCREENED,
                ivrs_date_screen_failed as IVRS_DATE_SCREEN_FAILED, 
                ivrs_date_randomized as IVRS_DATE_RANDOMIZED,
                ivrs_date_completed as IVRS_DATE_COMPLETED, 
                ivrs_date_re_screened as IVRS_DATE_RE_SCREENED,
                ivrs_date_pre_screened as IVRS_DATE_PRE_SCREENED, 
                ivrs_date_randomization_failed IVRS_DATE_RANDOMIZATION_FAILED,
                ivrs_date_pre_screened_failed as IVRS_DATE_PRE_SCREEN_FAILED, 
                ivrs_date_enrollment as IVRS_DATE_ENROLLMENT,
                ivrs_date_dropout as IVRS_DATE_DROPOUT, 
                acurian_enroll_date as ACURIAN_ENROLLED_DT,
                acurian_resolve_date as ACURIAN_RESOLVED_DT,
                acurian_consent_date as ACURIAN_CONSENTED_DT, 
                acurian_rand_date as ACURIAN_RANDOMIZED_DT,
                acurian_site_id as ACURIAN_SITE_ID, 
                ivrs_site_id as IVRS_SITE_ID, 
                ivrs_region as IVRS_REGION,
                system_rank as SYSTEM_RANK,
                ivrs_file_name as IVRS_FILE_NAME,
                '1' as CONFIRMATION_METHOD_CD
         FROM   rankedData
         WHERE  system_rank = 0
         """)

    //    matches.take(100).foreach(println)     
    updateInStaging(exactMatches)

    val stagingTable = null

    sql.sql("""
      SELECT rankedData.* 
      FROM rankedData
      JOIN(SELECT ivrs_project_id, ivrs_patient_id , ivrs_protocol_number , ivrs_country, MIN(system_rank) as mrank
      FROM rankedData
      GROUP BY ivrs_project_id, ivrs_patient_id , ivrs_protocol_number , ivrs_country
      ) AS minrank ON rankedData.ivrs_project_id = minrank.ivrs_project_id AND 
      rankedData.ivrs_patient_id = minrank.ivrs_patient_id AND
      rankedData.ivrs_protocol_number = minrank.ivrs_protocol_number AND
      rankedData.ivrs_country = minrank.ivrs_country AND
      rankedData.system_rank = minrank.mrank
      """).createOrReplaceTempView("forUniqueAcr")

    sql.sql("""
      SELECT forUniqueAcr.* 
      FROM forUniqueAcr
      JOIN(SELECT acurian_patient_id, MIN(system_rank) as mrank
      FROM forUniqueAcr
      GROUP BY acurian_patient_id
      ) AS minrank ON forUniqueAcr.acurian_patient_id = minrank.acurian_patient_id AND
      forUniqueAcr.system_rank = minrank.mrank
      """).createOrReplaceTempView("toRemoveCrossStudy")

    val ranksApplied = sql.sql("""
        SELECT * 
        FROM toRemoveCrossStudy 
        WHERE
        ivrs_project_id IN (SELECT ivrs_project_id 
                             FROM toRemoveCrossStudy 
                             GROUP BY ivrs_project_id 
                             having count(ivrs_project_id) = 1)
        OR
        ivrs_project_id IN (SELECT ivrs_project_id 
                             FROM toRemoveCrossStudy 
                             GROUP BY ivrs_project_id
                             HAVING SUM(CASE WHEN acurian_project_id = acurian_project_id
                                             THEN 1 ELSE 0 END) = 0)
        OR ivrs_project_id = acurian_project_id
        """)

    //    ranksApplied.filter(ranksApplied("system_rank") !== 0).take(100).foreach(println)

    val rejectedRecordsRemoved = removeRejectedRecords(ranksApplied.filter(ranksApplied("system_rank") !== 0))

    val rankedStats = calculateStats(rejectedRecordsRemoved)

    val dashBoardFiltered = dashBoardFilter(rankedStats, rankThreshold)

    dumpForDashboard(dashBoardFiltered._2, "dashboardfilerecords")
    dashBoardFiltered._1.createOrReplaceTempView("output")

    val autoMatch = sql.sql(s"""
     
     SELECT acurian_screening_id as ACURIAN_SSID,
            acurian_project_id as ACURIAN_PROJECT_ID, 
            ivrs_project_id as IVRS_PROJECT_ID, 
            acurian_consented_protocol as ACURIAN_PROTOCOL_NUM, 
            ivrs_protocol_number as IVRS_PROTOCOL_NUMBER, 
            ivrs_country as IVRS_COUNTRY,
            acurian_patient_id as ACURIAN_PATIENT_ID, 
            ivrs_patient_id as IVRS_PATIENT_ID,
            ivrs_dob_d as IVRS_DOB_DAY, 
            ivrs_dob_m as IVRS_DOB_MONTH,
            ivrs_dob_y as IVRS_DOB_YEAR, 
            ivrs_gender as IVRS_GENDER, 
            ivrs_pt_fi as IVRS_PATIENT_F_INITIAL, 
            ivrs_pt_mi as IVRS_PATIENT_M_INITIAL, 
            ivrs_pt_li as IVRS_PATIENT_L_INITIAL, 
            ivrs_invst_f_name as IVRS_INVESTIGATOR_F_INITIAL,
            ivrs_invst_m_name as IVRS_INVESTIGATOR_M_INITIAL, 
            ivrs_invst_l_name as IVRS_INVESTIGATOR_L_INITIAL,
            ivrs_date_screened as IVRS_DATE_SCREENED,
            ivrs_date_screen_failed as IVRS_DATE_SCREEN_FAILED, 
            ivrs_date_randomized as IVRS_DATE_RANDOMIZED,
            ivrs_date_completed as IVRS_DATE_COMPLETED, 
            ivrs_date_re_screened as IVRS_DATE_RE_SCREENED,
            ivrs_date_pre_screened as IVRS_DATE_PRE_SCREENED, 
            ivrs_date_randomization_failed IVRS_DATE_RANDOMIZATION_FAILED,
            ivrs_date_pre_screened_failed as IVRS_DATE_PRE_SCREEN_FAILED, 
            ivrs_date_enrollment as IVRS_DATE_ENROLLMENT,
            ivrs_date_dropout as IVRS_DATE_DROPOUT, 
            acurian_enroll_date as ACURIAN_ENROLLED_DT,
            acurian_resolve_date as ACURIAN_RESOLVED_DT,
            acurian_consent_date as ACURIAN_CONSENTED_DT, 
            acurian_rand_date as ACURIAN_RANDOMIZED_DT,
            acurian_site_id as ACURIAN_SITE_ID, 
            ivrs_site_id as IVRS_SITE_ID, 
            ivrs_region as IVRS_REGION,
            system_rank as SYSTEM_RANK,
            ivrs_file_name as IVRS_FILE_NAME,
            '2' as CONFIRMATION_METHOD_CD
     FROM   output
     """)

    updateInStaging(autoMatch)

    val fileToDelete = hdfsPath + "/acurianData/" + ivrsFileName.split('.')(0) + ".csv"
    val fs = FileSystem.get(HDFSFactory.conf)
    if (fs.exists(new Path(fileToDelete)))
      fs.delete(new Path(fileToDelete), true)

    dashBoardFiltered._2

  }

  def updateInStaging(data: DataFrame) = {
    data.rdd.foreachPartition(partition => {
      import sparkSession.implicits._
      val mongoConn = new MongoDBConnector
      mongoConn.connect(Constants.PROCESS_MONGO_IP, Constants.PROCESS_MONGO_PORT)
      val dataBase: com.mongodb.casbah.MongoDB = mongoConn.mongoClient("test")
      val collection = dataBase("acurianstagings")

      partition.foreach(jsonRow => {
        val rowMap = jsonRow.getValuesMap[Any](jsonRow.schema.fieldNames)
        mongoConn.updateAcurianStagingRecord("test", "acurianstagings", rowMap, jsonRow.getAs[String]("IVRS_PATIENT_ID"),
          jsonRow.getAs[String]("IVRS_PROTOCOL_NUMBER"),
          jsonRow.getAs[String]("IVRS_PROJECT_ID"),
          jsonRow.getAs[String]("IVRS_COUNTRY"))
      })
      mongoConn.closeMongoClient
    })
  }

  def writeReadAcurianData(data: DataFrame): DataFrame = {

    var writer: DataFrameWriter[Row] = null
    writer = data.write.format("com.databricks.spark.csv")
    writer.option("header", "true")
      .option("delimiter", ",")
      .save(hdfsPath + "/acurianDataToRead/")
    sql.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "false")
      .option("header", "true")
      .option("delimiter", ",")
      .load(hdfsPath + "/acurianDataToRead/")
  }

  def dumpForDashboard(data: DataFrame, collectionName: String) = {
    data.toJSON.rdd.foreachPartition(partition => {
      import sparkSession.implicits._
      val mongoConn = new MongoDBConnector
      mongoConn.connect(Constants.PROCESS_MONGO_IP, Constants.PROCESS_MONGO_PORT)
      val dataBase: com.mongodb.casbah.MongoDB = mongoConn.mongoClient("test")
      val collection = dataBase(collectionName)
      partition.foreach(jsonRow => {
        val jsonRecord = com.mongodb.util.JSON.parse(jsonRow).asInstanceOf[DBObject]
        collection.insert(jsonRecord)
      })
    })
  }

  def dashBoardFilter(data: DataFrame, systemRank: Int): (DataFrame, DataFrame) = {

    data.createOrReplaceTempView("StatCalculated")
    val autoMatch = sql.sql(s"""
      SELECT *
      FROM   StatCalculated
      WHERE  system_rank <= ${systemRank} AND
             acurian_rand_count < 1 AND
             acurian_patient_count <= 1 AND
             ivrs_patient_count <= 1
      """)

    data.except(autoMatch).createOrReplaceTempView("MatchForPm")
    val pmMatch = sql.sql(s"""
      SELECT *, "Pending" as status
      FROM   MatchForPm
      WHERE  system_rank <> 5000
      """)

    (autoMatch, pmMatch)
  }

  def calculateStats(data: DataFrame): DataFrame = {

    data.createOrReplaceTempView("rankedData")
    sql.sql(s"""
      
      SELECT rankedData.acurian_screening_id, rankedData.acurian_project_id, 
             rankedData.ivrs_project_id,rankedData.acurian_referred_protocol, rankedData.ivrs_region,
             rankedData.acurian_consented_protocol,rankedData.ivrs_protocol_number,
             rankedData.acurian_country,rankedData.ivrs_country,rankedData.acurian_enroll_date,
             rankedData.acurian_patient_id,rankedData.ivrs_patient_id,
             rankedData.acurian_dob_d,rankedData.acurian_dob_m,rankedData.acurian_dob_y,
             rankedData.acurian_gender,rankedData.acurian_pt_fi,rankedData.acurian_pt_mi,
             rankedData.acurian_pt_li,rankedData.ivrs_dob_d,rankedData.ivrs_dob_m,
             rankedData.ivrs_dob_y,rankedData.ivrs_gender,rankedData.ivrs_pt_fi,
             rankedData.ivrs_pt_mi,rankedData.ivrs_pt_li,rankedData.ivrs_invst_f_name,
             rankedData.ivrs_invst_m_name,rankedData.ivrs_invst_l_name,
             rankedData.acurian_invst_f_name,rankedData.acurian_invst_m_name,
             rankedData.acurian_invst_l_name,rankedData.ivrs_date_screened,
             rankedData.ivrs_date_screen_failed,rankedData.ivrs_date_randomized,
             rankedData.ivrs_date_completed,rankedData.ivrs_date_re_screened,
             rankedData.ivrs_date_pre_screened,rankedData.ivrs_date_randomization_failed,
             rankedData.ivrs_date_pre_screened_failed,rankedData.ivrs_date_enrollment,
             rankedData.ivrs_date_dropout,rankedData.acurian_released_date,
             rankedData.acurian_fov_date,rankedData.acurian_resolve_date,
             rankedData.acurian_consent_date,rankedData.acurian_rand_date,
             rankedData.acurian_site_id,rankedData.ivrs_site_id,rankedData.mapped_site_no,
             rankedData.system_rank,rankedData.rule,rankedData.ivrs_file_name,
             acrPtCount.acurian_patient_count,ivrsPtCount.ivrs_patient_count,randTable.acurian_rand_count,
             (CASE WHEN rankedData.acurian_project_id = rankedData.ivrs_project_id THEN true ELSE false END) as same_study_match
      FROM rankedData 
      JOIN  (SELECT acurian_patient_id , count(acurian_patient_id) as acurian_patient_count
      FROM rankedData
      GROUP BY acurian_patient_id)  
      AS acrPtCount on acrPtCount.acurian_patient_id = rankedData.acurian_patient_id
      JOIN  (SELECT ivrs_patient_id , count(ivrs_patient_id) as ivrs_patient_count
      FROM rankedData
      GROUP BY ivrs_patient_id)  
      AS ivrsPtCount on ivrsPtCount.ivrs_patient_id = rankedData.ivrs_patient_id
      JOIN (SELECT acurian_patient_id, count(CASE  WHEN acurian_rand_date IS NOT NULL THEN 1 END) as acurian_rand_count
      FROM rankedData
      GROUP BY acurian_patient_id)
      AS randTable on randTable.acurian_patient_id = rankedData.acurian_patient_id
      
      """)
  }

  def removeRejectedRecords(data: DataFrame): DataFrame = {

    val rejectedRemovedRows = data.rdd.mapPartitions(partition => {
      import sparkSession.implicits._
      val mongoConn = new MongoDBConnector
      mongoConn.connect(Constants.PROCESS_MONGO_IP, Constants.PROCESS_MONGO_PORT)
      val dataBase: com.mongodb.casbah.MongoDB = mongoConn.mongoClient("test")
      val rejectedCollection = dataBase("ivrsrejectedrecords")
      val inprocessCollection = dataBase("dashboardfilerecords")

      val newPartition = partition.filter(row => {
        val rejectedRecords = MongoDBObject("ivrs_protocol_number" -> row.getAs[String]("ivrs_protocol_number"),
          "ivrs_project_id" -> row.getAs[String]("ivrs_project_id"),
          "ivrs_patient_id" -> row.getAs[String]("ivrs_patient_id"),
          "ivrs_country" -> row.getAs[String]("ivrs_country"),
          "acurian_patient_id" -> row.getAs[java.math.BigDecimal]("acurian_patient_id").toString.split('.')(0))

        val inProcessRecords = MongoDBObject("ivrs_protocol_number" -> row.getAs[String]("ivrs_protocol_number"),
          "ivrs_project_id" -> row.getAs[String]("ivrs_project_id"),
          "ivrs_patient_id" -> row.getAs[String]("ivrs_patient_id"),
          "ivrs_country" -> row.getAs[String]("ivrs_country"))

        var flag = true
        val rejectedResult = rejectedCollection.findOne(rejectedRecords)
        val inProcessResult = inprocessCollection.findOne(inProcessRecords)

        if (rejectedResult != None) {
          if ((Json.parse(rejectedResult.getOrElse().toString) \ "system_rank").as[Int] >= row.getAs[Int]("system_rank"))
            flag = false
        } else if (inProcessResult != None)
          if ((Json.parse(inProcessResult.getOrElse().toString) \ "status").as[String].equals("In Process"))
            flag = false
        flag
      })
      newPartition
    })
    sql.applySchema(rejectedRemovedRows, rulesAppliedSchema)
  }

  def removeInProcessRecords(data: DataFrame): DataFrame = {

    val inProcessRemovedRows = data.rdd.mapPartitions(partition => {
      import sparkSession.implicits._
      val mongoConn = new MongoDBConnector
      mongoConn.connect(Constants.PROCESS_MONGO_IP, Constants.PROCESS_MONGO_PORT)
      val dataBase: com.mongodb.casbah.MongoDB = mongoConn.mongoClient("test")
      val collection = dataBase("dashboardfilerecords")

      val newPartition = partition.filter(row => {
        val mongoDbObj = MongoDBObject("ivrs_ssid" -> row.getAs[String]("ivrs_patient_id"),
          "ivrs_project" -> row.getAs[String]("ivrs_project_id"),
          "ivrs_protocol" -> row.getAs[String]("ivrs_protocol_number"),
          "acurian_patient_id" -> row.getAs[String]("acurian_screening_id"),
          "acurian_project" -> row.getAs[String]("acurian_project_id"),
          "acurian_protocol" -> row.getAs[String]("acurian_consented_protocol"))
        val result = collection.findOne(mongoDbObj)
        val status = (Json.parse(result.getOrElse().toString) \ "status").as[String]
        if (result == None)
          true
        else if ((Json.parse(result.getOrElse().toString) \ "status").as[String].equals("InProcess"))
          false
        else
          false
      })
      //mongoConn.closeMongoClient
      newPartition
    })
    sql.applySchema(inProcessRemovedRows, rulesAppliedSchema)

  }

  def preProcess(ivrsData: DataFrame, acurianData: DataFrame): DataFrame = {
    ivrsData.createOrReplaceTempView("preivrs")
    acurianData.createOrReplaceTempView("preacurian")

    sql.sql(s"SELECT * FROM preivrs WHERE NOT EXISTS " +
      s"(SELECT 1 FROM preacurian WHERE preivrs.${Constants.IVRS_PATIENT_ID} = preacurian.${Constants.ACURIAN_SCREENING_ID} AND " +
      s"preivrs.${Constants.IVRS_PROTOCOL_NUMBER} = preacurian.${Constants.ACURIAN_CONSENTED_PROTOCOL}")
  }

  def applyRanks(ivrsFileName: String, schemaAppliedData: DataFrame, acurianData: DataFrame): DataFrame = {

    //try {

    val seriesID = ivrsFileName.split("-").dropRight(1).mkString("-")
    val schemaMapping = mongoCon.getRecordsByKey(Constants.PROCESS_MONGO_DB_NAME, "ivrsprojectfileseries", "series_id", seriesID)

    val sites = schemaAppliedData.select(schemaAppliedData(Constants.IVRS_SITE_ID))
      .distinct.collect.map(_.toSeq.mkString).map(site => {
        s"'${site}'"
      }).mkString(",")

    var prop = new java.util.Properties
    val url = getConnectionString("S_NUMTRA", "S_NUMTRA#2018", "prd-db-scan.acurian.com",
      "1521", "acuprd_app_numtra.acurian.com")

    val facilityQuery = s"""(
      SELECT c.facility_cd, c.site_num 
      FROM s_study.study a, S_ACUTRACK.PROTOCOL b, s_site.study_site_protocol c 
      WHERE a.project_code = '${ivrsFileName.split("-")(0).replace("OUS", "")}'
      AND a.study_id = c.study_id 
      AND b.ACUSCREEN_PROTOCOL_ID = c.PROTOCOL_ID 
      AND b.protocol_number = '${ivrsFileName.split("-")(1)}' 
      AND c.site_num in (${sites}))
    """

    var facMap = scala.collection.immutable.Map.empty[String, String]
    val facilityDf = sql.read.jdbc(url, facilityQuery, prop).rdd.
      foreach(record => {
        if (record.getAs[String](1) != null && record.getAs[String](0) != null)
          facMap += (record.getAs[String](1) -> record.getAs[String](0))
      })

    val jsonValue = Json.parse(schemaMapping)

    val fileSchema = (jsonValue \ "file_schema").as[Map[String, JsValue]] //.toSeq

    //      if ((jsonValue \ "rules_id").as[Seq[JsValue]] == null)
    //        throw new Exception("No rules found for this file series")

    val rulesId = (jsonValue \ "rules_id").as[Seq[JsValue]].map(ruleId => {
      (ruleId \ "$oid").as[String]
    })
    val rules = rulesId.map(id => {
      Json.parse(mongoCon.getRecordsById("test", "datawranglerrules", id))
    })

    val convertToFormat = (date: String, format: String) => {
      if (date != null)
        Try(new Timestamp(DateTimeFormat.forPattern(format)
          .parseDateTime(date)
          .getMillis)) getOrElse null
      else
        null
    }
    val fileSeries = Json.parse(mongoCon.getRecordsByKey(Constants.PROCESS_MONGO_DB_NAME, "ivrsprojectfileseries", "series_id", seriesID))
    val projectStartDate = convertToFormat((fileSeries \ "project_start_date").get.toString.split("T")(0).drop(10), "yyyy-MM-dd")
    val ivrsRunStartDate = convertToFormat((fileSeries \ "ivrs_run_start_date").get.toString.split("T")(0).drop(10), "yyyy-MM-dd")

    var ranks = rules.map(rule => {

      val attrbs = (rule \ "rules").as[Seq[JsValue]]
      var dob = ""
      var gender = ""
      var initials = ""
      var investiName = ""
      var siteID = ""
      attrbs.foreach(f => {
        (f \ "attribute").as[String] match {
          case "Date of Birth"     => dob = (f \ "rule_alias").as[Seq[String]].mkString(",")
          case "Gender"            => gender = (f \ "rule_alias").as[Seq[String]].mkString(",")
          case "Initials"          => initials = (f \ "rule_alias").as[Seq[String]].mkString(",")
          case "Site ID"           => siteID = (f \ "rule_alias").as[Seq[String]].mkString(",")
          case "Investigator Name" => investiName = (f \ "rule_alias").as[Seq[String]].mkString(",")
          case _                   => ""
        }
      })
      Row.fromSeq(Seq(dob, gender, initials, siteID, investiName, (rule \ "rank").as[Int]))
    })

    val ranksSchema = new StructType()
      .add(StructField("DateOfBirth", StringType, true))
      .add(StructField("Gender", StringType, true))
      .add(StructField("Initials", StringType, true))
      .add(StructField("SiteID", StringType, true))
      .add(StructField("InvestigatorName", StringType, true))
      .add(StructField("Rank", IntegerType, true))

    val ranksTable = sql.applySchema(sparkSession.sparkContext.parallelize(ranks), ranksSchema).collect

    val acurianCollectedData = acurianData.collect //SparkFactory.sc.broadcast(acurianData)

    //dumpForDashboard(acurianCollectedData, "acurianCollectedData")

    val cartesianProduct = schemaAppliedData.crossJoin(acurianData)

    val ranksApplied = cartesianProduct.rdd.map(record => {
      val ivrsDobD = record.getAs[String](Constants.IVRS_DOB_DAY)
      val ivrsDobM = record.getAs[String](Constants.IVRS_DOB_MONTH)
      val ivrsDobY = record.getAs[String](Constants.IVRS_DOB_YEAR)

      val ivrsPtFI = record.getAs[String](Constants.IVRS_PATIENT_F_INITIAL)
      val ivrsPtMI = record.getAs[String](Constants.IVRS_PATIENT_M_INITIAL)
      val ivrsPtLI = record.getAs[String](Constants.IVRS_PATIENT_L_INITIAL)

      val ivrsGender = record.getAs[String](Constants.IVRS_GENDER)

      val ivrsSiteID = record.getAs[String](Constants.IVRS_SITE_ID)

      val ivrsInvstFName = record.getAs[String](Constants.IVRS_INVESTIGATOR_F_NAME)
      val ivrsInvstMName = record.getAs[String](Constants.IVRS_INVESTIGATOR_M_NAME)
      val ivrsInvstLName = record.getAs[String](Constants.IVRS_INVESTIGATOR_L_NAME)

      var rankedRecords: Seq[Row] = Seq.empty
      var recordRank = 5000
      var recordMatch: Seq[Row] = Seq.empty
      var countryMatch = true
      var datesMatch = true
      var alreadyMatchMatch = false
      var releaseDateMatch = true

      val seqMatches = (seqA: Seq[String], seqB: Seq[String]) => {
        var count = 0
        for (i <- 0 until seqA.length) {
          if (seqA(i) != null && seqB(i) != null)
            if (!seqA(i).isEmpty && !seqB(i).isEmpty)
              if (seqA(i).equals(seqB(i))) count += 1
        }
        count
      }

      val countNulls = (seqA: Seq[String]) => {
        var count = 0
        for (i <- 0 until seqA.length) {
          if (seqA(i) == null)
            count += 1
        }
        count
      }

      var getAppliedRule = (dobMatch: String, initialsMatch: String, genderMatch: String,
        siteIDMatch: String, investiNameMatch: String) => {

        var rule: String = ""

        dobMatch match {
          case "missing"  => rule += "Date Of Birth missing,"
          case "1of3"     => rule += "Date Of Birth 1 of 3 match,"
          case "2of3"     => rule += "Date Of Birth 2 of 3 match,"
          case "exact"    => rule += "Date Of Birth exact match,"
          case "notmatch" => rule += "Date Of Birth does not match,"
          case "1of3T"    => rule += "Date Of Birth 1 of 3 transpose,"
          case "2of3T"    => rule += "Date Of Birth 2 of 3 transpose,"
          case "1of3F"    => rule += "Date Of Birth 1 of 3 fuzzy,"
          case "2of3F"    => rule += "Date Of Birth 2 of 3 fuzzy,"
          case "3of3T"    => rule += "Date Of Birth 3 of 3 transpose,"
        }

        initialsMatch match {
          case "missing"   => rule += "Pt Initials missing,"
          case "exact"     => rule += "Pt Initials exact match,"
          case "notmatch"  => rule += "Pt Initials does not match,"
          case "transpose" => rule += "Pt Initials transpose,"
        }

        investiNameMatch match {
          case "missing"  => rule += "Investigator Name missing,"
          case "1of3"     => rule += "Investigator Name 1 of 3 match,"
          case "2of3"     => rule += "Investigator Name 2 of 3 match,"
          case "exact"    => rule += "Investigator Name exact match,"
          case "notmatch" => rule += "Investigator Name does not match,"
        }

        genderMatch match {
          case "missing"  => rule += "Gender missing,"
          case "exact"    => rule += "Gender exact match,"
          case "notmatch" => rule += "Gender does not match,"
        }

        siteIDMatch match {
          case "missing"  => rule += "SiteID missing"
          case "exact"    => rule += "SiteID exact match"
          case "notmatch" => rule += "SiteID does not match,"
        }

        rule
      }

      var dobMatch = "missing"
      var genderMatch = "missing"
      var ptNameMatch = "missing"
      var siteIdMatch = "missing"
      var investiNameMatch = "missing"

      val acurianDobD = Try(record.getAs[Int](Constants.ACURIAN_DOB_DAY).toString) getOrElse null
      val acurianDobM = Try(record.getAs[Int](Constants.ACURIAN_DOB_MONTH).toString) getOrElse null
      val acurianDobY = Try(record.getAs[Int](Constants.ACURIAN_DOB_YEAR).toString) getOrElse null

      val acurianPtFI = Try(record.getAs[String](Constants.ACURIAN_PATIENT_F_INITIAL).replaceAll("\\s", "")(0).toString) getOrElse null
      val acurianPtMI = Try(record.getAs[String](Constants.ACURIAN_PATIENT_M_INITIAL).replaceAll("\\s", "")(0).toString) getOrElse null
      val acurianPtLI = Try(record.getAs[String](Constants.ACURIAN_PATIENT_L_INITIAL).replaceAll("\\s", "")(0).toString) getOrElse null

      val acurianGender = Try(record.getAs[String](Constants.ACURIAN_GENDER).replaceAll("\\s", "")) getOrElse null

      val acurianSiteID = Try(record.getAs[String](Constants.ACURIAN_SITE_ID).replaceAll("\\s", "")) getOrElse null

      val acurianInvstFName = Try(record.getAs[String](Constants.ACURIAN_INVESTIGATOR_F_NAME).replaceAll("\\s", "")) getOrElse null
      val acurianInvstMName = Try(record.getAs[String](Constants.ACURIAN_INVESTIGATOR_M_NAME).replaceAll("\\s", "")) getOrElse null
      val acurianInvstLName = Try(record.getAs[String](Constants.ACURIAN_INVESTIGATOR_L_NAME).replaceAll("\\s", "")) getOrElse null

      println(s"acurianDobD: ${acurianDobD}")
      println(s"acurianDobM: ${acurianDobM}")
      println(s"acurianDobY: ${acurianDobY}")
      println(s"acurianPtFI: ${acurianPtFI}")
      println(s"acurianPtMI: ${acurianPtMI}")
      println(s"acurianPtLI: ${acurianPtLI}")
      println(s"acurianGender: ${acurianGender}")
      println(s"acurianSiteID: ${acurianSiteID}")
      println(s"acurianInvstFName: ${acurianInvstFName}")
      println(s"acurianInvstMName: ${acurianInvstMName}")
      println(s"acurianInvstLName: ${acurianInvstLName}")
      println("===================================================")
      
      /**
       * DOB Match
       */

      if (ivrsDobD != null || ivrsDobM != null || ivrsDobY != null)
        dobMatch = seqMatches(Seq(ivrsDobD, ivrsDobM, ivrsDobY),
          Seq(acurianDobD, acurianDobM, acurianDobY)) match {
            case 0 => {
              val transposeCount = seqMatches(Seq(ivrsDobM, ivrsDobD, ivrsDobY),
                Seq(acurianDobD, acurianDobM, acurianDobY))
              if (transposeCount == 0) {
                "notmatch"
              } else {
                transposeCount match {
                  case 1 => {
                    val nullCount = countNulls(Seq(ivrsDobM, ivrsDobD, ivrsDobY))
                    if (nullCount == 2)
                      "1of3T"
                    else
                      "notmatch"
                  }
                  case 2 => {
                    val nullCount = countNulls(Seq(ivrsDobM, ivrsDobD, ivrsDobY))
                    if (nullCount == 1)
                      "2of3T"
                    else
                      "notmatch"
                  }
                  case 3 => "3of3T"
                }
              }
            }
            case 1 => {
              val transposeCount = seqMatches(Seq(ivrsDobM, ivrsDobD, ivrsDobY),
                Seq(acurianDobD, acurianDobM, acurianDobY))
              if (transposeCount <= 1) {
                val nullCount = countNulls(Seq(ivrsDobM, ivrsDobD, ivrsDobY))
                if (nullCount == 2) {
                  "1of3"
                } else {
                  "1of3F"
                }
              } else {
                transposeCount match {
                  case 2 => {
                    val nullCount = countNulls(Seq(ivrsDobM, ivrsDobD, ivrsDobY))
                    if (nullCount == 1)
                      "2of3T"
                    else
                      "notmatch"
                  }
                  case 3 => "3of3T"
                }
              }
            }
            case 2 => {
              val transposeCount = seqMatches(Seq(ivrsDobM, ivrsDobD, ivrsDobY),
                Seq(acurianDobD, acurianDobM, acurianDobY))
              if (transposeCount <= 2) {
                val nullCount = countNulls(Seq(ivrsDobM, ivrsDobD, ivrsDobY))
                if (nullCount == 1) {
                  "2of3"
                } else {
                  "2of3F"
                }
              } else {
                transposeCount match {
                  case 3 => "3of3T"
                }
              }
            }
            case 3 => "exact"
          }

      /**
       * Pt Name Match
       */
      if (ivrsPtFI != null || ivrsPtMI != null || ivrsPtLI != null)
        ptNameMatch = s"${ivrsPtFI}${ivrsPtMI}${ivrsPtLI}".equals(s"${acurianPtFI}${acurianPtMI}${acurianPtLI}") match {
          case true => "exact"
          case false => {
            val transpose = s"${ivrsPtLI}${ivrsPtMI}${ivrsPtFI}".equals(s"${acurianPtFI}${acurianPtMI}${acurianPtLI}")
            transpose match {
              case true  => "transpose"
              case false => "notmatch"
            }

          }
        }

      /**
       * Gender Match
       */
      if (ivrsGender != null && acurianGender != null)
        genderMatch = ivrsGender.charAt(0).toLower.equals(acurianGender.charAt(0).toLower) match {
          case true => "exact"
          case _    => "notmatch"
        }

      /**
       * SiteID Match
       */
      if (ivrsSiteID != null && acurianSiteID != null)
        siteIdMatch = acurianSiteID.toLowerCase.equals(ivrsSiteID.toLowerCase) ||
          acurianSiteID.toLowerCase.equals(facMap.get(ivrsSiteID).getOrElse("").toLowerCase) match {
            case true => "exact"
            case _    => "notmatch"
          }

      /**
       * Investigator Name Match
       */
      if (ivrsInvstFName != null || ivrsInvstLName != null || ivrsInvstMName != null)
        investiNameMatch = seqMatches(Seq(ivrsInvstFName, ivrsInvstMName, ivrsInvstLName),
          Seq(acurianInvstFName, acurianInvstMName, acurianInvstLName)) match {
            case 0 => "notmatch"
            case 1 => "1of3"
            case 2 => "2of3"
            case 3 => "exact"
          }

      /**
       * Country Match
       */
      if (record.getAs[String](Constants.IVRS_COUNTRY) != null) {
        countryMatch = record.getAs[String](Constants.IVRS_COUNTRY) == record.getAs[String](Constants.ACURIAN_COUNTRY)
      }

      /**
       * Dates Match
       */
      if (record.getAs[String](Constants.IVRS_DATE_SCREENED) != null) {
        datesMatch = record.getAs[Timestamp](Constants.IVRS_DATE_SCREENED).after(projectStartDate) &&
          record.getAs[Timestamp](Constants.IVRS_DATE_SCREENED).after(ivrsRunStartDate)
      }

      /**
       * Already Match Check
       */
      val acurianScrnID = Try(record.getAs[String](Constants.ACURIAN_SCREENING_ID)) getOrElse "acurian_screening_id"
      val ivrsPtID = Try(record.getAs[String](Constants.IVRS_PATIENT_ID)) getOrElse "ivrs_patient_id"
      val acurianProjectID = Try(record.getAs[String](Constants.ACURIAN_PROJECT_ID)) getOrElse "acurian_protocol_no"
      val ivrsProjectID = Try(record.getAs[String](Constants.IVRS_PROJECT_ID)) getOrElse "ivrs_protocol_no"

      val idCheck = Try(acurianScrnID.equals(ivrsPtID)) getOrElse false
      val projCheck = Try(acurianProjectID.equals(ivrsProjectID)) getOrElse false

      if (idCheck && projCheck) {
        alreadyMatchMatch = true
      }

      /**
       * Released Date Match
       */
      if (record.getAs[String](Constants.IVRS_DATE_SCREENED) != null) {
        releaseDateMatch = record.getAs[Timestamp](Constants.IVRS_DATE_SCREENED).after(record.getAs[Timestamp](Constants.ACURIAN_RELEASED_DATE))
      }

      ranksTable.foreach(rule => {
        if (rule.getAs[String]("DateOfBirth").equals(dobMatch) &&
          rule.getAs[String]("Gender").equals(genderMatch) &&
          rule.getAs[String]("Initials").equals(ptNameMatch) &&
          (rule.getAs[String]("SiteID").split(",").contains(siteIdMatch) ||
            rule.getAs[String]("InvestigatorName").split(",").contains(investiNameMatch)))
          recordRank = rule.getAs[Int]("Rank")
      })

      val rule = getAppliedRule(dobMatch, ptNameMatch, genderMatch, siteIdMatch, investiNameMatch)
      if (countryMatch == false || datesMatch == false)
        recordRank = 5000
      if (releaseDateMatch == false)
        recordRank = 5000
      if (alreadyMatchMatch == true) {
        recordRank = 0
      }
      
      Row.fromSeq(Seq(record.getAs[String](Constants.ACURIAN_SCREENING_ID), // Acurian Project ID
        record.getAs[String](Constants.ACURIAN_PROJECT_ID), // Acurian Project ID
        record.getAs[String](Constants.IVRS_PROJECT_ID), // IVRS Project ID
        record.getAs[String](Constants.ACURIAN_REFERRED_PROTOCOL), // Acurian Referred Protocol Number
        record.getAs[String](Constants.ACURIAN_CONSENTED_PROTOCOL), // Acurian Consented Protocol Number
        record.getAs[String](Constants.IVRS_PROTOCOL_NUMBER), //IVRS Protocol Number
        record.getAs[String](Constants.ACURIAN_COUNTRY), //Acurian Country
        record.getAs[String](Constants.IVRS_COUNTRY), //IVRS Country
       record.getAs[String](Constants.ACURIAN_PATIENT_ID), // Acurian Patient ID
        record.getAs[String](Constants.IVRS_PATIENT_ID), //IVRS Screening ID
        record.getAs[String](Constants.IVRS_REGION), //IVRS Screening ID
        acurianDobD, //Acurian DOB day
        acurianDobM, //Acurian DOB month
        acurianDobY, //Acurian DOB year
        acurianGender, //Acurian Gender
        acurianPtFI, //Acurian Patient First Initial
        acurianPtMI, //Acurian Patient Middle Initial
        acurianPtLI, //Acurian Patient Last Initial
        ivrsDobD, //IVRS DOB day
        ivrsDobM, //IVRS DOB month
        ivrsDobY, //IVRS DOB year
        ivrsGender, //IVRS Gender
        ivrsPtFI, //IVRS Patient First Initial
        ivrsPtMI, //IVRS Patient Middle Initial
        ivrsPtLI, //IVRS Patient Last Initial
        ivrsInvstFName, //IVRS inv first name
        ivrsInvstMName, //IVRS inv middle name
        ivrsInvstLName, //IVRS inv last name
        acurianInvstFName, //Acurian inv first name
        acurianInvstMName, //Acurian inv middle name
        acurianInvstLName, //Acurian inv last name
        record.getAs[Timestamp](Constants.IVRS_DATE_SCREENED),
        record.getAs[Timestamp](Constants.IVRS_DATE_SCREEN_FAILED),
        record.getAs[Timestamp](Constants.IVRS_DATE_RANDOMIZED),
        record.getAs[Timestamp](Constants.IVRS_DATE_COMPLETED),
        record.getAs[Timestamp](Constants.IVRS_DATE_RE_SCREENED),
        record.getAs[Timestamp](Constants.IVRS_DATE_PRE_SCREENED),
        record.getAs[Timestamp](Constants.IVRS_DATE_RANDOMIZATION_FAILED),
        record.getAs[Timestamp](Constants.IVRS_DATE_PRE_SCREENED_FAILED),
        record.getAs[Timestamp](Constants.IVRS_DATE_ENROLLMENT),
        record.getAs[Timestamp](Constants.IVRS_DATE_DROPOUT),
        record.getAs[Timestamp](Constants.ACURIAN_RELEASED_DATE),
        record.getAs[Timestamp](Constants.ACURIAN_ENROLL_DATE),
        record.getAs[Timestamp](Constants.ACURIAN_FOV_DATE),
        record.getAs[Timestamp](Constants.ACURIAN_RESOLVE_DATE),
        record.getAs[Timestamp](Constants.ACURIAN_CONSENT_DATE),
        record.getAs[Timestamp](Constants.ACURIAN_RAND_DATE),
        acurianSiteID, //Acurian site ID
        ivrsSiteID, // IVRS site ID
        "", //ivrs mapped site no
        recordRank,
        rule,
        ivrsFileName))

    })

    sql.applySchema(ranksApplied, rulesAppliedSchema)
    //    } catch {
    //      case e: Exception => {
    //        writeException(e, ivrsFileName)
    //        null
    //      }
    //    }
  }

  def mapSchema(ivrsFileName: String, ivrsData: DataFrame): DataFrame = {

    //    try {
    val seriesID = ivrsFileName.split("-").dropRight(1).mkString("-")

    val schemaMapping = mongoCon.getRecordsByKey(Constants.PROCESS_MONGO_DB_NAME, "ivrsprojectfileseries", "series_id", seriesID)

    //      if (schemaMapping == null)
    //        throw new Exception("Schema mapping not found for this file series")

    val jsonValue = Json.parse(schemaMapping)

    val fileSchema = (jsonValue \ "file_schema").as[Map[String, JsValue]] //.toSeq

    val rulesId = (jsonValue \ "rules_id").as[Seq[JsValue]].map(ruleId => {
      (ruleId \ "$oid").as[String]
    })
    val rules = rulesId.map(id => {
      Json.parse(mongoCon.getRecordsById("test", "datawranglerrules", id))
    })

    val rawData = ivrsData.rdd.map(row => {

      val convertToFormat = (date: String, format: String) => {
        if (date != null)
          Try(new Timestamp(DateTimeFormat.forPattern(format)
            .parseDateTime(date)
            .getMillis)) getOrElse null
        else
          null
      }

      val monthToInt = (month: String) => {
        month.toLowerCase match {
          case "jan" => "1"
          case "feb" => "2"
          case "mar" => "3"
          case "apr" => "4"
          case "may" => "5"
          case "jun" => "6"
          case "jul" => "7"
          case "aug" => "8"
          case "sep" => "9"
          case "oct" => "10"
          case "nov" => "11"
          case "dec" => "12"
        }
      }

      /**
       * ivrs patient dob
       */
      var dobD: String = null
      var dobM: String = null
      var dobY: String = null

      /**
       * ivrs patient name
       */
      var patientFInitial: String = null
      var patientMInitial: String = null
      var patientLInitial: String = null

      /**
       * ivrs Investigator name
       */
      var investigatorFInitial: String = null
      var investigatorMInitial: String = null
      var investigatorLInitial: String = null

      /**
       * ivrs visit dates
       */
      var dateScreened: Timestamp = null
      var dateScreenFailed: Timestamp = null
      var dateRandomized: Timestamp = null
      var dateCompleted: Timestamp = null
      var dateReScreened: Timestamp = null
      var datePreScreened: Timestamp = null
      var dateRandomizationFailed: Timestamp = null
      var datePreScreenFailed: Timestamp = null
      var dateEnrollment: Timestamp = null
      var dateDropOut: Timestamp = null

      /**
       * ivrs visit dates array
       */
      val datesList: Seq[String] = Seq(Constants.IVRS_DATE_COMPLETED, Constants.IVRS_DATE_DROPOUT,
        Constants.IVRS_DATE_ENROLLMENT, Constants.IVRS_DATE_PRE_SCREENED, Constants.IVRS_DATE_PRE_SCREENED_FAILED,
        Constants.IVRS_DATE_RANDOMIZATION_FAILED, Constants.IVRS_DATE_RANDOMIZED, Constants.IVRS_DATE_RE_SCREENED,
        Constants.IVRS_DATE_SCREEN_FAILED, Constants.IVRS_DATE_SCREENED)

      /**
       * ivrs country
       */
      var country: String = null

      /**
       * forced value map
       */
      var forcedValuesMap = Map.empty[String, String]

      /**
       * One To One map
       */
      var oneToOneFieldsMap = Map.empty[String, String]

      fileSchema.toSeq.foreach(field => {
        val isMapped = (field._2 \ "is_mapped").as[String].toBoolean
        val format = (field._2 \ "format").asOpt[String]
        val forcedValue = (field._2 \ "forced_value").asOpt[String]
        val mappingFields = (field._2 \ "mapping_field").asOpt[Seq[String]]
        val isSplitted = (field._2 \ "is_splitted").asOpt[String]

        if (isMapped || !forcedValue.isEmpty) {
          /**
           * If there is a format
           */

          if (!format.isEmpty && !mappingFields.isEmpty) {

            if (datesList.contains(mappingFields.get(0))) {
              mappingFields.get(0) match {
                case "date_screened" => {
                  //                    if (row.getAs[String](field._1) == null)
                  //                      throw new Exception("Date Screened Not Found in Schema")
                  dateScreened = convertToFormat(row.getAs[String](field._1), format.get.toString.replace("yyyy", "yy"))
                }
                case "date_screen_failed" => {
                  //                    if (row.getAs[String](field._1) == null)
                  //                      throw new Exception("Date Screen Failed Not Found in Schema")
                  dateScreenFailed = convertToFormat(row.getAs[String](field._1), format.get.toString.replace("yyyy", "yy"))
                }
                case "date_randomized" => {
                  //                    if (row.getAs[String](field._1) == null)
                  //                      throw new Exception("Date Randomized Not Found in Schema")
                  dateRandomized = convertToFormat(row.getAs[String](field._1), format.get.toString.replace("yyyy", "yy"))
                }
                case "date_completed" => {
                  //                    if (row.getAs[String](field._1) == null)
                  //                      throw new Exception("Date Completed Not Found in Schema")
                  dateCompleted = convertToFormat(row.getAs[String](field._1), format.get.toString.replace("yyyy", "yy"))
                }
                case "date_re_screened" => {
                  //                    if (row.getAs[String](field._1) == null)
                  //                      throw new Exception("Date Re Screened Not Found in Schema")
                  dateReScreened = convertToFormat(row.getAs[String](field._1), format.get.toString.replace("yyyy", "yy"))
                }
                case "date_pre_screened" => {
                  //                    if (row.getAs[String](field._1) == null)
                  //                      throw new Exception("Date Pre Screened Not Found in Schema")
                  datePreScreened = convertToFormat(row.getAs[String](field._1), format.get.toString.replace("yyyy", "yy"))
                }
                case "date_randomization_failed" => {
                  //                    if (row.getAs[String](field._1) == null)
                  //                      throw new Exception("Date Randomization Failed Not Found in Schema")
                  dateRandomizationFailed = convertToFormat(row.getAs[String](field._1), format.get.toString.replace("yyyy", "yy"))
                }
                case "date_pre_screen_failed" => {
                  //                    if (row.getAs[String](field._1) == null)
                  //                      throw new Exception("Date Pre Screen Failed Not Found in Schema")
                  datePreScreenFailed = convertToFormat(row.getAs[String](field._1), format.get.toString.replace("yyyy", "yy"))
                }
                case "date_enrollment" => {
                  //                    if (row.getAs[String](field._1) == null)
                  //                      throw new Exception("Date Enrollment Not Found in Schema")
                  dateEnrollment = convertToFormat(row.getAs[String](field._1), format.get.toString.replace("yyyy", "yy"))
                }
                case "date_dropout" => {
                  //                    if (row.getAs[String](field._1) == null)
                  //                      throw new Exception("Date Dropout Not Found in Schema")
                  dateDropOut = convertToFormat(row.getAs[String](field._1), format.get.toString.replace("yyyy", "yy"))
                }
                case _ => ""

              }
            } else {
              val formatItems = format.get.toString.toLowerCase.split(Constants.MAGIC_REGEX)
              if (row.getAs[String](field._1) != null) {
                val value = row.getAs[String](field._1).split(Constants.MAGIC_REGEX)
                formatItems.foreach(item => {
                  if (item.contains("dd")) {
                    dobD = value(formatItems.indexOf(item, 0))
                  } else if (item.contains("mm")) {
                    if (item.toLowerCase.equals("mmm")) {
                      if (value(formatItems.indexOf(item, 0)).matches("[a-zA-Z]+"))
                        dobM = monthToInt(value(formatItems.indexOf(item, 0)))
                    } else {
                      dobM = value(formatItems.indexOf(item, 0))
                    }
                  } else if (item.contains("yy")) {
                    dobY = value(formatItems.indexOf(item, 0))
                  } else if (item.contains("f")) {
                    if (mappingFields.get.contains("investigator_f_initial")
                      && formatItems.indexOf(item, 0) < value.size) {
                      investigatorFInitial = value(formatItems.indexOf(item, 0))
                    }
                  } else if (item.contains("m")) {
                    if (mappingFields.get.contains("investigator_m_initial")
                      && formatItems.indexOf(item, 0) < value.size)
                      investigatorMInitial = value(formatItems.indexOf(item, 0))
                  } else if (item.contains("l")) {
                    if (mappingFields.get.contains("investigator_l_initial")
                      && formatItems.indexOf(item, 0) < value.size)
                      investigatorLInitial = value(formatItems.indexOf(item, 0))
                  }

                  /**
                   * Patient F Initial
                   */
                  if (item.contains("f") && mappingFields.get.contains("patient_f_initial")
                    && row.getAs[String](field._1) != null) {
                    if (value.length == 1) {
                      if (value(0).length == 3) {
                        patientFInitial = row.getAs[String](field._1)(formatItems.indexOf(item, 0)).toString
                      } else if (value(0).length == 2) {
                        if (formatItems.indexOf(item, 0) == formatItems.length - 1) {
                          patientFInitial = row.getAs[String](field._1)(formatItems.indexOf(item, 0) - 1).toString
                        } else {
                          patientFInitial = row.getAs[String](field._1)(0).toString
                        }
                      }
                    } else if (value.length == 2) {
                      if (formatItems.indexOf(item, 0) == formatItems.length - 1) {
                        patientFInitial = value(formatItems.indexOf(item, 0) - 1)
                      } else {
                        patientFInitial = value(formatItems.indexOf(item, 0))
                      }
                    } else if (value.length == 1) {
                      patientFInitial = value(0)
                    }
                  } /**
                   * Patient M Initial
                   */ else if (item.contains("m") && mappingFields.get.contains("patient_m_initial")
                    && row.getAs[String](field._1) != null) {
                    if (value.length == 1 && value(0).length == 3) {
                      patientMInitial = row.getAs[String](field._1)(formatItems.indexOf(item, 0)).toString
                    } else if (value.length == 3) {
                      patientMInitial = value(formatItems.indexOf(item, 0))
                    }
                  }

                  /**
                   * Patient L Initial
                   */

                  if (item.contains("l") && mappingFields.get.contains("patient_l_initial")
                    && row.getAs[String](field._1) != null) {
                    if (value.length == 1) {
                      if (value(0).length == 3) {
                        patientLInitial = row.getAs[String](field._1)(formatItems.indexOf(item, 0)).toString
                      } else if (value(0).length == 2) {
                        if (formatItems.indexOf(item, 0) == formatItems.length - 1) {
                          patientLInitial = row.getAs[String](field._1)(formatItems.indexOf(item, 0) - 1).toString
                        }
                      }
                    } else if (value.length == 2) {
                      if (formatItems.indexOf(item, 0) == formatItems.length - 1) {
                        patientLInitial = value(formatItems.indexOf(item, 0) - 1)
                      } else {
                        patientLInitial = value(formatItems.indexOf(item, 0))
                      }
                    }
                  }
                })
              }
            }
          }

          /**
           * If there is a forced value
           */
          if (!forcedValue.isEmpty) {
            forcedValuesMap += (field._1 -> forcedValue.get.toString)
          }

          /**
           * If there is a one to one Mapping
           */
          if (!mappingFields.isEmpty) {
            if (mappingFields.get.size == 1) {
              oneToOneFieldsMap += (mappingFields.get(0) -> field._1)
            }
          }

          /**
           * If there is a split value
           */
          if (!isSplitted.isEmpty) {
            val visit = row.getAs[String](field._1)
            val splittedField = (field._2 \ "value_of_splitted_field").as[String]
            val splitMapping = (field._2 \ "splitted_field_mapping").as[Map[String, String]]
            val dateFormat = (field._2 \ "format").as[String]
            val dateValue = row.getAs[String](splittedField)
            if (splitMapping.keys.toSeq.contains(visit))
              splitMapping.get(visit).get match {
                case "date_screened"             => dateScreened = convertToFormat(dateValue, dateFormat.replace("yyyy", "yy"))
                case "date_screen_failed"        => dateScreenFailed = convertToFormat(dateValue, dateFormat.replace("yyyy", "yy"))
                case "date_randomized"           => dateRandomized = convertToFormat(dateValue, dateFormat.replace("yyyy", "yy"))
                case "date_completed"            => dateCompleted = convertToFormat(dateValue, dateFormat.replace("yyyy", "yy"))
                case "date_re_screened"          => dateReScreened = convertToFormat(dateValue, dateFormat.replace("yyyy", "yy"))
                case "date_pre_screened"         => datePreScreened = convertToFormat(dateValue, dateFormat.replace("yyyy", "yy"))
                case "date_randomization_failed" => dateRandomizationFailed = convertToFormat(dateValue, dateFormat.replace("yyyy", "yy"))
                case "date_pre_screen_failed"    => datePreScreenFailed = convertToFormat(dateValue, dateFormat.replace("yyyy", "yy"))
                case "date_enrollment"           => dateEnrollment = convertToFormat(dateValue, dateFormat.replace("yyyy", "yy"))
                case "date_dropout"              => dateDropOut = convertToFormat(dateValue, dateFormat.replace("yyyy", "yy"))
                case _                           => ""
              }
          }
        }
      })

      var protocolNumber: String = null
      var projectID: String = null
      var region: String = null
      var gender: String = null
      var siteID: String = null

      var countryCodes = Map.empty[String, String]
      Locale.getISOCountries.foreach(x => {
        val l = new Locale("", x)
        countryCodes += (l.getDisplayCountry -> x)
      })

      if (oneToOneFieldsMap.keys.toSeq.contains("protocol_number")) {
        protocolNumber = row.getAs[String](oneToOneFieldsMap.get("protocol_number").get)
      } else {
        protocolNumber = forcedValuesMap.get("protocol_number").get
      }

      if (fileSchema.contains("project_id")) {
        projectID = seriesID.split("-")(0)
      } else {
        projectID = row.getAs[String](oneToOneFieldsMap.get("project_id").get)
      }

      if (oneToOneFieldsMap.keys.toSeq.contains("country")) {
        //          if (countryCodes.getOrElse(country, null) == null)
        //            throw new Exception(s"Country: ${country} is not a valid country")
        country = row.getAs[String](oneToOneFieldsMap.get("country").get)
        if (country != null)
          if (country.length > 2) {
            country = countryCodes.get(country).get
          }
      } else {
        country = forcedValuesMap.get("country").get
        if (country.length > 2) {
          country = countryCodes.get(country).get
        }
      }

      if (fileSchema.contains("region")) {
        region = seriesID.split("-")(2)
      } else {
        region = row.getAs[String](oneToOneFieldsMap.get("region").get)
      }

      if (dobY != null) {
        if (dobY.length == 2) {
          val dateString = Calendar.getInstance.getTime.toString
          val cYear = dateString.substring(dateString.length - 2).toInt
          val pYear = (dobY(0) + dobY(1)).toInt
          if (pYear <= cYear) {
            dobY = "20" + dobY
          } else {
            dobY = "19" + dobY
          }

        }
      }

      if (dobD == null) {
        if (oneToOneFieldsMap.keys.toSeq.contains("dob_day")) {
          dobD = row.getAs[String](oneToOneFieldsMap.get("dob_day").get)
        }
      }

      if (dobM == null) {
        if (oneToOneFieldsMap.keys.toSeq.contains("dob_month")) {
          dobM = row.getAs[String](oneToOneFieldsMap.get("dob_month").get)
        }
      }

      if (dobY == null) {
        if (oneToOneFieldsMap.keys.toSeq.contains("dob_year")) {
          dobY = row.getAs[String](oneToOneFieldsMap.get("dob_year").get)
        }
      }

      if (patientFInitial == null) {
        if (oneToOneFieldsMap.keys.toSeq.contains("patient_f_initial")) {
          patientFInitial = row.getAs[String](oneToOneFieldsMap.get("patient_f_initial").get)
        }
      }

      if (patientMInitial == null) {
        if (oneToOneFieldsMap.keys.toSeq.contains("patient_m_initial")) {
          patientMInitial = row.getAs[String](oneToOneFieldsMap.get("patient_m_initial").get)
        }
      }

      if (patientLInitial == null) {
        if (oneToOneFieldsMap.keys.toSeq.contains("patient_l_initial")) {
          patientLInitial = row.getAs[String](oneToOneFieldsMap.get("patient_l_initial").get)
        }
      }

      if (investigatorFInitial == null) {
        if (oneToOneFieldsMap.keys.toSeq.contains("investigator_f_initial") &&
          row.getAs[String](oneToOneFieldsMap.get("investigator_f_initial").get) != null) {
          investigatorFInitial = row.getAs[String](oneToOneFieldsMap.get("investigator_f_initial").get)
        }
      }

      if (investigatorMInitial == null) {
        if (oneToOneFieldsMap.keys.toSeq.contains("investigator_m_initial") &&
          row.getAs[String](oneToOneFieldsMap.get("investigator_m_initial").get) != null) {
          investigatorMInitial = row.getAs[String](oneToOneFieldsMap.get("investigator_m_initial").get)
        }
      }
      if (investigatorLInitial == null) {
        if (oneToOneFieldsMap.keys.toSeq.contains("investigator_l_initial") &&
          row.getAs[String](oneToOneFieldsMap.get("investigator_l_initial").get) != null) {
          investigatorLInitial = row.getAs[String](oneToOneFieldsMap.get("investigator_l_initial").get)
        }
      }

      if (oneToOneFieldsMap.keys.toSeq.contains("gender") &&
        row.getAs[String](oneToOneFieldsMap.get("gender").get) != null) {
        gender = row.getAs[String](oneToOneFieldsMap.get("gender").get)(0).toString
      }

      if (oneToOneFieldsMap.keys.toSeq.contains("site_id") &&
        row.getAs[String](oneToOneFieldsMap.get("site_id").get) != null) {
        siteID = row.getAs[String](oneToOneFieldsMap.get("site_id").get)
      }

      Row.fromSeq(Seq(row.getAs[String](oneToOneFieldsMap.get("patient_id").get),
        protocolNumber, projectID, siteID, region, country, patientFInitial, patientMInitial,
        patientLInitial, gender, dobD, dobM, dobY, investigatorFInitial, investigatorMInitial,
        investigatorLInitial, dateScreened, dateScreenFailed, dateRandomized, dateCompleted,
        dateReScreened, datePreScreened, dateRandomizationFailed, datePreScreenFailed,
        dateEnrollment, dateDropOut))
    })

    val ivrsSchema = new StructType()
      .add(StructField(Constants.IVRS_PATIENT_ID, StringType, true))
      .add(StructField(Constants.IVRS_PROTOCOL_NUMBER, StringType, true))
      .add(StructField(Constants.IVRS_PROJECT_ID, StringType, true))
      .add(StructField(Constants.IVRS_SITE_ID, StringType, true))
      .add(StructField(Constants.IVRS_REGION, StringType, true))
      .add(StructField(Constants.IVRS_COUNTRY, StringType, true))
      .add(StructField(Constants.IVRS_PATIENT_F_INITIAL, StringType, true))
      .add(StructField(Constants.IVRS_PATIENT_M_INITIAL, StringType, true))
      .add(StructField(Constants.IVRS_PATIENT_L_INITIAL, StringType, true))
      .add(StructField(Constants.IVRS_GENDER, StringType, true))
      .add(StructField(Constants.IVRS_DOB_DAY, StringType, true))
      .add(StructField(Constants.IVRS_DOB_MONTH, StringType, true))
      .add(StructField(Constants.IVRS_DOB_YEAR, StringType, true))
      .add(StructField(Constants.IVRS_INVESTIGATOR_F_NAME, StringType, true))
      .add(StructField(Constants.IVRS_INVESTIGATOR_M_NAME, StringType, true))
      .add(StructField(Constants.IVRS_INVESTIGATOR_L_NAME, StringType, true))
      .add(StructField(Constants.IVRS_DATE_SCREENED, TimestampType, true))
      .add(StructField(Constants.IVRS_DATE_SCREEN_FAILED, TimestampType, true))
      .add(StructField(Constants.IVRS_DATE_RANDOMIZED, TimestampType, true))
      .add(StructField(Constants.IVRS_DATE_COMPLETED, TimestampType, true))
      .add(StructField(Constants.IVRS_DATE_RE_SCREENED, TimestampType, true))
      .add(StructField(Constants.IVRS_DATE_PRE_SCREENED, TimestampType, true))
      .add(StructField(Constants.IVRS_DATE_RANDOMIZATION_FAILED, TimestampType, true))
      .add(StructField(Constants.IVRS_DATE_PRE_SCREENED_FAILED, TimestampType, true))
      .add(StructField(Constants.IVRS_DATE_ENROLLMENT, TimestampType, true))
      .add(StructField(Constants.IVRS_DATE_DROPOUT, TimestampType, true))

    sql.applySchema(rawData, ivrsSchema).groupBy(
      Constants.IVRS_PATIENT_ID, Constants.IVRS_PROTOCOL_NUMBER, Constants.IVRS_PROJECT_ID,
      Constants.IVRS_SITE_ID, Constants.IVRS_REGION, Constants.IVRS_COUNTRY, Constants.IVRS_PATIENT_F_INITIAL,
      Constants.IVRS_PATIENT_M_INITIAL, Constants.IVRS_PATIENT_L_INITIAL,
      Constants.IVRS_GENDER, Constants.IVRS_DOB_DAY, Constants.IVRS_DOB_MONTH,
      Constants.IVRS_DOB_YEAR, Constants.IVRS_INVESTIGATOR_F_NAME, Constants.IVRS_INVESTIGATOR_M_NAME,
      Constants.IVRS_INVESTIGATOR_L_NAME).agg(
        first(Constants.IVRS_DATE_SCREENED, true) as Constants.IVRS_DATE_SCREENED,
        first(Constants.IVRS_DATE_SCREEN_FAILED, true) as Constants.IVRS_DATE_SCREEN_FAILED,
        first(Constants.IVRS_DATE_RANDOMIZED, true) as Constants.IVRS_DATE_RANDOMIZED,
        first(Constants.IVRS_DATE_COMPLETED, true) as Constants.IVRS_DATE_COMPLETED,
        first(Constants.IVRS_DATE_RE_SCREENED, true) as Constants.IVRS_DATE_RE_SCREENED,
        first(Constants.IVRS_DATE_PRE_SCREENED, true) as Constants.IVRS_DATE_PRE_SCREENED,
        first(Constants.IVRS_DATE_RANDOMIZATION_FAILED, true) as Constants.IVRS_DATE_RANDOMIZATION_FAILED,
        first(Constants.IVRS_DATE_PRE_SCREENED_FAILED, true) as Constants.IVRS_DATE_PRE_SCREENED_FAILED,
        first(Constants.IVRS_DATE_ENROLLMENT, true) as Constants.IVRS_DATE_ENROLLMENT,
        first(Constants.IVRS_DATE_DROPOUT, true) as Constants.IVRS_DATE_DROPOUT)
    //    }
    //  catch {
    //      case e: Exception => {
    //        writeException(e, ivrsFileName)
    //        null
    //      }
    //    }
  }

  def loadIvrsData(ivrsFileName: String, headerLines: Int): DataFrame = {
    //  try {
    var filePath = Constants.IVRS_FILES_PATH + s"/${ivrsFileName}"

    //      if (ivrsFileName.contains("xls") && !ivrsFileName.contains("xlsx"))
    //        throw new Exception(".xls format not supported")

    if (ivrsFileName.contains("xlsx")) {
      val rdd = convertToRdd(HDFSFactory.conf, hdfsPath + filePath, headerLines, ivrsFileName)
      var fileToDelete = hdfsPath + Constants.IVRS_FILES_PATH + "/temp"
      val fs = FileSystem.get(HDFSFactory.conf)
      if (fs.exists(new Path(fileToDelete)))
        fs.delete(new Path(fileToDelete), true)
      rdd.saveAsTextFile(hdfsPath + Constants.IVRS_FILES_PATH + "/temp")

      fileToDelete = hdfsPath + "/acurianData"
      if (fs.exists(new Path(fileToDelete)))
        fs.delete(new Path(fileToDelete), true)

      merge(hdfsPath + Constants.IVRS_FILES_PATH + "/temp", hdfsPath + "/acurianData/" + ivrsFileName.split('.')(0) + ".csv")
      filePath = hdfsPath + "/acurianData/" + ivrsFileName.split('.')(0) + ".csv"
    } else if (ivrsFileName.contains("csv")) {
      val rdd = sparkSession.sparkContext.textFile(hdfsPath + filePath).mapPartitionsWithIndex {
        (idx, iter) => if (idx == 0) iter.drop(headerLines) else iter
      }
      var fileToDelete = hdfsPath + Constants.IVRS_FILES_PATH + "/temp"
      val fs = FileSystem.get(HDFSFactory.conf)
      if (fs.exists(new Path(fileToDelete)))
        fs.delete(new Path(fileToDelete), true)
      rdd.saveAsTextFile(hdfsPath + Constants.IVRS_FILES_PATH + "/temp")
      fileToDelete = hdfsPath + "/acurianData"
      if (fs.exists(new Path(fileToDelete)))
        fs.delete(new Path(fileToDelete), true)
      merge(hdfsPath + Constants.IVRS_FILES_PATH + "/temp", hdfsPath + "/acurianData/" + ivrsFileName.split('.')(0) + ".csv")
      filePath = hdfsPath + "/acurianData/" + ivrsFileName.split('.')(0) + ".csv"
    }

    sql.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "false")
      .option("header", "true")
      .option("delimiter", ",")
      .load(filePath)
    //    } catch {
    //      case e: Exception => {
    //        writeException(e, ivrsFileName)
    //        null
    //      }
    //    }
  }

  def convertToRdd(hadoopConf: Configuration, inputFile: String, headerLines: Int, ivrsFileName: String): RDD[String] = {
    // load using the new Hadoop API (mapreduce.*)
    //try {
    val sc = sparkSession.sparkContext
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val exists = fs.exists(new org.apache.hadoop.fs.Path(inputFile))
    //      if (!exists) {
    //        throw new Exception("File Does not Exist")
    //      }
    val excelRDD = sc.newAPIHadoopFile(inputFile, classOf[ExcelFileInputFormat],
      classOf[Text], classOf[ArrayWritable], hadoopConf)
    // print the cell address and the formatted cell content
    excelRDD.map(hadoopKeyValueTuple => {
      // note see also org.zuinnote.hadoop.office.format.common.converter.ExcelConverterSimpleSpreadSheetCellDAO to convert from and to SpreadSheetCellDAO
      val rowStrBuffer = new StringBuilder
      var i = 0;
      for (x <- hadoopKeyValueTuple._2.get) { // parse through the SpreadSheetCellDAO
        if (x != null) {
          rowStrBuffer.append(s""""${x.asInstanceOf[SpreadSheetCellDAO].getFormattedValue}",""")
        } else {
          rowStrBuffer.append(",")
        }
        i += 1
      }
      if (rowStrBuffer.length > 0) {
        rowStrBuffer.deleteCharAt(rowStrBuffer.length - 1) // remove last comma
      }
      rowStrBuffer.toString
    }).mapPartitionsWithIndex(
      (index, it) => if (index == 0) it.drop(headerLines) else it,
      preservesPartitioning = true)
    //    } catch {
    //      case e: Exception => {
    //        writeException(e, ivrsFileName)
    //        null
    //      }
    //    }
  }

  def merge(srcPath: String, dstPath: String): Unit = {
    val hdfs = FileSystem.get(HDFSFactory.conf)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true,
      HDFSFactory.conf, null)
  }

  def stagingSchema(): StructType = {
    new StructType()
      .add(StructField("IVRS_PROJECT_ID", StringType, true))
      .add(StructField("IVRS_PROTOCOL_NUMBER", StringType, true))
      .add(StructField("IVRS_PATIENT_ID", StringType, true))
      .add(StructField("IVRS_COUNTRY", StringType, true))
      .add(StructField("ACURIAN_PROJECT_ID", StringType, true))
      .add(StructField("ACURIAN_PATIENT_ID", IntegerType, true))
      .add(StructField("SYSTEM_RANK", IntegerType, false))
      .add(StructField("CONFIRMATION_METHOD", IntegerType, true))
  }

  def rulesAppliedSchema(): StructType = {
    new StructType()
      .add(StructField("acurian_screening_id", StringType, true))
      .add(StructField("acurian_project_id", StringType, true))
      .add(StructField("ivrs_project_id", StringType, true))
      .add(StructField("acurian_referred_protocol", StringType, true))
      .add(StructField("acurian_consented_protocol", StringType, true))
      .add(StructField("ivrs_protocol_number", StringType, true))
      .add(StructField("acurian_country", StringType, true))
      .add(StructField("ivrs_country", StringType, true))
      .add(StructField("acurian_patient_id", StringType, true))
      .add(StructField("ivrs_patient_id", StringType, true))
      .add(StructField("ivrs_region", StringType, true))
      .add(StructField("acurian_dob_d", StringType, true))
      .add(StructField("acurian_dob_m", StringType, true))
      .add(StructField("acurian_dob_y", StringType, true))
      .add(StructField("acurian_gender", StringType, true))
      .add(StructField("acurian_pt_fi", StringType, true))
      .add(StructField("acurian_pt_mi", StringType, true))
      .add(StructField("acurian_pt_li", StringType, true))
      .add(StructField("ivrs_dob_d", StringType, true))
      .add(StructField("ivrs_dob_m", StringType, true))
      .add(StructField("ivrs_dob_y", StringType, true))
      .add(StructField("ivrs_gender", StringType, true))
      .add(StructField("ivrs_pt_fi", StringType, true))
      .add(StructField("ivrs_pt_mi", StringType, true))
      .add(StructField("ivrs_pt_li", StringType, true))
      .add(StructField("ivrs_invst_f_name", StringType, true))
      .add(StructField("ivrs_invst_m_name", StringType, true))
      .add(StructField("ivrs_invst_l_name", StringType, true))
      .add(StructField("acurian_invst_f_name", StringType, true))
      .add(StructField("acurian_invst_m_name", StringType, true))
      .add(StructField("acurian_invst_l_name", StringType, true))
      .add(StructField("ivrs_date_screened", TimestampType, true))
      .add(StructField("ivrs_date_screen_failed", TimestampType, true))
      .add(StructField("ivrs_date_randomized", TimestampType, true))
      .add(StructField("ivrs_date_completed", TimestampType, true))
      .add(StructField("ivrs_date_re_screened", TimestampType, true))
      .add(StructField("ivrs_date_pre_screened", TimestampType, true))
      .add(StructField("ivrs_date_randomization_failed", TimestampType, true))
      .add(StructField("ivrs_date_pre_screened_failed", TimestampType, true))
      .add(StructField("ivrs_date_enrollment", TimestampType, true))
      .add(StructField("ivrs_date_dropout", TimestampType, true))
      .add(StructField("acurian_released_date", TimestampType, true))
      .add(StructField("acurian_enroll_date", TimestampType, true))
      .add(StructField("acurian_fov_date", TimestampType, true))
      .add(StructField("acurian_resolve_date", TimestampType, true))
      .add(StructField("acurian_consent_date", TimestampType, true))
      .add(StructField("acurian_rand_date", TimestampType, true))
      .add(StructField("acurian_site_id", StringType, true))
      .add(StructField("ivrs_site_id", StringType, true))
      .add(StructField("mapped_site_no", StringType, true))
      .add(StructField("system_rank", IntegerType, true))
      .add(StructField("rule", StringType, true))
      .add(StructField("ivrs_file_name", StringType, true))
  }

  def getConnectionString(userName: String, password: String, host: String, port: String, dbName: String): String = {
    s"jdbc:oracle:thin:${userName}/${password}@${host}:${port}/${dbName}"
  }

}