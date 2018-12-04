package test.test

object Constants {
  //<<<<<<< HEAD
  var configurationLoaded = false

  /** execution engine **/
  var SPARK_VERSION: String = "1.6.0"
  var HADOOP_VERSION: String = "2.6+"
  val APPLICATION_SERVER: String = "applicationServer"

  /** Multi Cluster Config **/
  val PLATFORM_LOG_DIR: String = "/platalytics/logs/"
  val DEFAULT_REQUEST_HANDLER_NAME: String = "requestHandler"
  val DEFAULT_REMOTE_REF_NAME: String = "remoteRef"
  var CLUSTER_TAG: String = "compute"
  val COMPUTE_TYPE: String = "compute"
  val API_TYPE: String = "api"
  val STORAGE_TYPE: String = "storage"
  val STAT_TYPE: String = "application"
  var DEFAULT_JOB_SERVER_IP: String = "localhost"
  var DEFAULT_JOB_SERVER_PORT: String = "5070"
  var DEFAULT_API_SERVER_IP: String = "cloud5-server"
  var DEFAULT_API_SERVER_PORT: String = "5050"
  var DEFAULT_STAT_SERVER_IP: String = "cloud5-server"
  var DEFAULT_STAT_SERVER_PORT: String = "5060"
  var DEFAULT_APPLICATION_SERVER_PORT: String = "2554"
  var COMPUTE_SERVICES: String = "compute-services-svc"
  var API_SERVICES: String = "api-services-svc"
  var STAT_SERVICES: String = "application-services-svc"

  //** ivrs master schema fields **//
  val IVRS_PROJECT_ID = "project_id"
  val IVRS_PATIENT_ID = "ivrs_patient_id"
  val IVRS_SITE_ID = "site_id"
  val IVRS_DOB_DAY = "dob_day"
  val IVRS_DOB_MONTH = "dob_month"
  val IVRS_DOB_YEAR = "dob_year"
  val IVRS_PATIENT_F_INITIAL = "patient_f_initial"
  val IVRS_PATIENT_M_INITIAL = "patient_m_initial"
  val IVRS_PATIENT_L_INITIAL = "patient_l_initial"
  val IVRS_INVESTIGATOR_F_NAME = "investigator_f_name"
  val IVRS_INVESTIGATOR_M_NAME = "investigator_m_name"
  val IVRS_INVESTIGATOR_L_NAME = "investigator_l_name"
  val IVRS_DATE_SCREENED = "date_screened"
  val IVRS_DATE_SCREEN_FAILED = "date_screen_failed"
  val IVRS_DATE_RANDOMIZED = "date_randomized"
  val IVRS_DATE_COMPLETED = "date_completed"
  val IVRS_DATE_RE_SCREENED = "date_re_screened"
  val IVRS_DATE_PRE_SCREENED = "date_pre_screened"
  val IVRS_DATE_RANDOMIZATION_FAILED = "date_randomization_failed"
  val IVRS_DATE_PRE_SCREENED_FAILED = "date_pre_screen_failed"
  val IVRS_DATE_ENROLLMENT = "date_enrollment"
  val IVRS_DATE_DROPOUT = "date_dropout"
  val IVRS_COUNTRY = "country"
  val IVRS_GENDER = "gender"
  val IVRS_REGION = "region"
  val IVRS_PROTOCOL_NUMBER = "protocol_number"

  //** Acurian RDBMS fields **//
  val ACURIAN_PROJECT_ID = "PROJECT_NO"
  val ACURIAN_PATIENT_ID = "ACR_PATIENT_ID"
  val ACURIAN_SCREENING_ID = "SCREENING_ID"
  val ACURIAN_SITE_ID = "SITE_NO"
  val ACURIAN_DOB_DAY = "ACR_DOB_DAY"
  val ACURIAN_DOB_MONTH = "ACR_DOB_MONTH"
  val ACURIAN_DOB_YEAR = "ACR_DOB_YEAR"
  val ACURIAN_PATIENT_F_INITIAL = "PT_FIRST_NAME"
  val ACURIAN_PATIENT_M_INITIAL = "PT_MIDDLE_NAME"
  val ACURIAN_PATIENT_L_INITIAL = "PT_LAST_NAME"
  val ACURIAN_REFERRED_PROTOCOL = "REFERRED_PROTOCOLS"
  val ACURIAN_RELEASED_DATE = "RELEASE_DT"
  val ACURIAN_ENROLL_DATE = "ENROLL_DT"
  val ACURIAN_FOV_DATE = "FOV_DATE"
  val ACURIAN_CONSENT_DATE = "CONSENT_DT"
  val ACURIAN_RAND_DATE = "RAND_DT"
  val ACURIAN_RESOLVE_DATE = "RESOLVED_DT"
  val ACURIAN_INVESTIGATOR_F_NAME = "INV_FIRST_NAME"
  val ACURIAN_INVESTIGATOR_M_NAME = "INV_MIDDLE_NAME"
  val ACURIAN_INVESTIGATOR_L_NAME = "INV_LAST_NAME"
  val ACURIAN_INVESTIGATOR_SUFFIX = "INV_SUFFIX"
  val ACURIAN_COUNTRY = "PT_COUNTRY"
  val ACURIAN_GENDER = "PT_GENDER"
  val ACURIAN_REGION = "PT_REGION"
  val ACURIAN_CONSENTED_PROTOCOL = "ACR_PROTOCOL_NUMBER"

  val ACURIAN_STAGING = "acurianstagings"

  //** stage types and sub types **//*
  val FULL_DEBUG: String = "full_debug"
  val SOURCE: String = "source"
  val IOT: String = "iot"
  val SINK: String = "sink"
  val ANALYTICS: String = "analytics"
  val TRAINING: String = "training"
  val TYPE_DATA_SOURCE: String = "connector"
  val TYPE_TRANSFORMATION: String = "transformation"
  val TYPE_JOIN: String = "join"
  val TYPE_ANALYTICS: String = "analytics"
  val TYPE_CORE: String = "core"
  val TYPE_PROCESSOR: String = "processor"
  val TYPE_SOURCE: String = "source"
  val TYPE_SINK: String = "sink"
  val TYPE_CLEANSING: String = "data_cleansing"
  val S3_SOURCE: String = "s3_source"
  val URL_SOURCE: String = "url_source"
  val STAGING_SINK_SOURCE: String = "datalake_source"
  val STREAMING_SINK_SOURCE: String = "streamingsink_source"
  val KAFKA_SOURCE: String = "kafka_source"
  val KAFKA_SINK: String = "kafka_sink"
  val KRAKEN_SOURCE: String = "kraken_source"
  val PARQUET_SINK: String = "parquet_sink"
  val STREAMING_SINK: String = "streaming_sink"
  val STAGING_SINK: String = "datalake_sink"
  val SQL_SOURCE: String = "sql_source"
  val IVRS_DUMP_SOURCE: String = "acurianStaging_source"
  val SQL_SINK: String = "sql_sink"
  val SQLSERVER_SINK: String = "sqlServer_sink"
  val SQLSERVER_SOURCE: String = "sqlServer_source"
  val MAP_SOURCE: String = "map_source"
  val S3_SINK: String = "s3_sink"
  val API_SOURCE = "api_source"
  val HDFS_SINK: String = "HDFS_sink"
  val HBASE_SINK: String = "HBase_sink"
  val FILE_SOURCE: String = "file_source"
  val AMAZON_SQS_SOURCE: String = "amazonsqs_source"
  val AMAZON_SQS_SINK: String = "amazonsqs_sink"
  val DUMMY_SOURCE: String = "dummy_source"
  val HDFS_SOURCE: String = "HDFS_source"
  val IVRS_RM_SOURCE: String = "ivrsRdbms_source"
  val HBASE_SOURCE: String = "HBase_source"
  val HIVE_SOURCE: String = "Hive_source"
  val AEROSPIKE_SOURCE: String = "aerospike_source"
  val PREDEFINED_SOURCE: String = "predefined_source"
  val FTP_SOURCE: String = "FTP_Source"
  val MONGODB_SOURCE: String = "mongodb_source"
  val MONGODB_SINK: String = "mongodb_sink"
  val USERDATA_SOURCE: String = "userdata_source"
  val USERDATA_SINK: String = "userdata_sink"
  val AEROSPIKE_SINK: String = "aerospike_sink"
  val HANA_SOURCE: String = "hana_source"
  val FLUME_SOURCE: String = "flume_source"
  val RSSFEEDS_SOURCE: String = "rssfeeds_source"
  val HANA_SINK: String = "hana_sink"
  val GIS_SINK: String = "gis_sink"
  val TWITTER_SOURCE: String = "twitter_source"
  val MQTT_SOURCE: String = "mqtt_iot"
  val MQTTHUB_SOURCE: String = "mqtt_hub_iot"
  val AMQP_SOURCE: String = "amqp_iot"
  val AMQPHUB_SOURCE: String = "amqp_hub_iot"
  val COAP_SOURCE: String = "coap_iot"
  val COAPHUB_SOURCE: String = "coap_hub_iot"
  val STOMP_SOURCE: String = "stomp_iot"
  val STOMPHUB_SOURCE: String = "stomp_hub_iot"
  val AZURE_SOURCE: String = "azure_iot"
  val AZUREHUB_SOURCE: String = "azure_hub_iot"
  val ACTIVEMQ_SOURCE: String = "activemq_iot"
  val ACTIVEMQHUB_SOURCE: String = "activemq_hub_iot"
  val AWS_SOURCE: String = "aws_iot"
  val AWSHUB_SOURCE: String = "aws_hub_iot"
  val IBM_SOURCE: String = "ibm_iot"
  val IBMHUB_SOURCE: String = "ibm_hub_iot"
  val REDSHIFT_SOURCE: String = "redshift_source"
  val REDSHIFT_SINK: String = "redshift_sink"
  val IVRS_RULES_ENGINE: String = "ivrs_rules_engine_transformation"
  val MIN: String = "min_transformation"
  val MAX: String = "max_transformation"
  val AVG: String = "avg_transformation"
  val SUM: String = "sum_transformation"
  val COUNT: String = "count_transformation"
  val FILTER: String = "filter_transformation"
  val ENCRYPTION: String = "encryption_transformation"
  val SPLIT: String = "split_transformation"
  val FORMULA: String = "formula_transformation"
  val MERGE: String = "merge_transformation"
  val SORT: String = "sort_transformation"
  val SKEW_JOIN: String = "skew_join_transformation"
  val BLOCK_JOIN: String = "block_join_transformation"
  val INNER_JOIN: String = "inner"
  val RIGHT_OUTER: String = "right_outer"
  val LEFT_OUTER: String = "left_outer"
  val COMPRESSION: String = "compression_transformation"
  val TAG: String = "tag_transformation"
  val AGGREGATION: String = "aggregation_transformation"
  val FIND_REPLACE: String = "find_replace_transformation"
  val UNION: String = "union_transformation"
  val TEXTUAL_EXTRACTION: String = "textual_feature_extraction_transformation"
  val OPTIMIZED_JOIN: String = "optimizedjoin_transformation"
  val RENAME: String = "rename_transformation"
  val WRANGLER: String = "datawrangler_datapreprocessing"
  val GIS_JOIN: String = "join_gis_transformation"
  val GIS_BUFFER: String = "buffer_gis_transformation"
  val GIS_FILTER: String = "filter_gis_transformation"
  val GIS_FILTER_CONTAINS: String = "contains_filter_gis_transformation"
  val GIS_FILTER_CROSSES: String = "crosses_filter_gis_transformation"
  val GIS_FILTER_WITHIN: String = "within_filter_gis_transformation"
  val GIS_FILTER_OVERLAPS: String = "overlaps_filter_gis_transformation"
  val GIS_FILTER_EQUALS: String = "equals_filter_gis_transformation"
  val GIS_FILTER_DISJOINT: String = "disjoint_filter_gis_transformation"
  val GIS_FILTER_TOUCHES: String = "touches_filter_gis_transformation"
  val GIS_GEOMETRY_CONVERSION: String = "geo_conversion_gis_transformation"
  val GIS_FLATTEN: String = "transformation_flatten_gis_transformation"
  val GIS_FILTER_INRIX: String = "filter_inrix_gis_transformation"
  val GIS_AREA: String = "area_transformation_gis_transformation"
  val GIS_LENGTH: String = "length_transformation_gis_transformation"
  val GIS_FETCH_FROM_ELASTIC_SEARCH = "elastic_search_fetch"
  val GIS_FETCH_FROM_ELASTIC_SEARCH_WITH_QUERY = "elastic_search_fetch_query"
  val GIS_FETCH_TILES = "hbase_tiling"
  val GIS_FETCH_TILES_TEMPORAL = "hbase_tiling_temporal"
  val GIS_DISTANCE: String = "distance_transformation_gis_transformation"
  val KINESIS_SOURCE: String = "kinesis_source"
  val KINESIS_SINK: String = "kinesis_sink"
  val SMART_SINK: String = "smart_sink"
  val CASSANDRA_SOURCE = "cassandra_source"
  val CASSANDRA_SINK = "cassandra_sink"
  val DISCARD = "discard_data_cleansing"
  val DATE_TIME_TRANS = "dateTime_transformation"
  val FILLING = "filling_transformation"
  val GIS_UNION: String = "union_gis_transformation"
  val GIS_INTERSECTION: String = "intersection_gis_transformation"
  val GIS_DIFFERENCE: String = "difference_gis_transformation"
  val GIS_SYMMETRIC_DIFFERENCE: String = "symmetric_difference_gis_transformation"
  val GIS_INTERSECTS: String = "intersects_filter_gis_transformation"
  val GIS_SPATIAL_JOIN: String = "spatial_join_gis_transformation"
  val GIS_AGGREGATION: String = "aggregation_gis_transformation"
  val GIS_GEOCODING: String = "geocoding_gis_transformation"
  val GIS_REVERSE_GEOCODING: String = "reverse_geocoding_gis_transformation"
  val GIS_INRIX_TAR: String = "inrix_gis_transformation"

  /** Cluster Sink Config **/
  val DEFAULT_SINK_IP: String = "cloud5-server"
  val DEFAULT_SINK_PORT: String = "9000"
  val DEFAULT_SINK_TYPE: String = STAGING_SINK
  val DEFAULT_SINK_LIMIT: Int = 500

  /** Monitoring **/
  val DATA_POINTS_COUNT: Int = 500

  /** yarn configurations **/
  val HDFS_LOCAL_RESOURCES_APP_DIR: String = "/platalytics/apps/" // hdfs path

  /** version management **/
  val LOCAL_CODEBASE_VERSION_PATH: String = "/platalytics/libs/default" // local path
  val APPLICATION_JAR_NAME: String = "processor-0.0.1-platform-processor-jar-with-dependencies.jar"

  /** user information **/
  var USER_NAME: String = null

  /** front end application**/
  var API_ACCESS_KEY: String = null
  var FRONTEND_HOST: String = null

  /** process stages **/
  var PROCESS_MONGO_IP: String = "172.16.248.25"
  //var PROCESS_MONGO_IP: String = "172.16.248.23"
  //var PROCESS_MONGO_IP: String = "ds-dev-node-04.acurian.com"
  var PROCESS_MONGO_PORT: String = "9876"
  var PROCESS_MONGO_DB_NAME: String = "test"
  val PROCESS_MONGO_COLLECTION_NAME_APPLICATION: String = "applications"
  val PROCESS_MONGO_COLLECTION_NAME_PROCESS: String = "rpipelines"
  val PROCESS_MONGO_COLLECTION_NAME_PROCESS_VERSIONS: String = "pipelineversions"
  val PROCESS_MONGO_COLLECTION_NAME_STAGES: String = "rstages"
  val PROCESS_MONGO_COLLECTION_NAME_PROFILES: String = "credentialprofiles"
  val PROCESS_MONGO_COLLECTION_NAME_STAGES_VERSIONS: String = "stageversions"
  val PARAMS_MONGO_COLLECTION_NAME: String = "parameters"
  val USERS_MONGO_COLLECTION_NAME: String = "users"
  val TEST_PIPELINES_COLLECTION_NAME: String = "testpipelines"
  val CHECKPOINTING_MONGO_COLLECTION_NAME = "checkpointing"
  val CHECKPOINTING_MONGO_DB_NAME = "checkpointdb"
  val CLUSTERS_INFO_MONGO_COLLECTION_NAME: String = "clustersdatas"
  val SAMPLE_DATA: String = "executionsampledatas"
  val PROCESS_LOGS_DB_NAME: String = "platformlogs"
  val PROCESS_LOGS_COLLECTION_NAME: String = "pipelinelogs"
  val STAGE_LOGS_COLLECTION_NAME: String = "stagelogs"
  val LOAD_FROM_MONGO: String = "mongo"
  val LOAD_FROM_HDFS: String = "hdfs"

  /** resource settings **/
  var NAME_NODE: String = "ds-dev-node-01:9000"
  //  var JOB_TRACKER: String = null
  //  var OOZIE_URL: String = null
  //  var SQOOP_URL: String = null
  var SPARK_URL: String = null
  var SPARK_UI_URL: String = null
  var SPARK_HOME: String = null
  var SPARK_EXECUTOR_MEMORY: String = "2g"
  var SPARK_EXECUTOR_CORES: String = "4"
  var SPARK_DEPLOY_MODE: String = "client"
  var SPARK_CONFIGURATIONS: Map[String, String] = Map.empty[String, String]
  var APPLICATION_NAME: String = "PlatalyticsJob"
  var PROCESS_ID: String = ""
  val YARN_CLIENT: String = "yarn-client"
  val YARN_CLUSTER: String = "yarn-cluster"
  val PROCESS_MONGO_COLLECTION_NAME_EXTERNALSOURCES: String = "externalsources"

  /** paths **/
  var PROCESS_APPS_BASE_PATH: String = null
  var HDFS_FILE_UPLOAD_BASE_PATH: String = null
  var TEMP_APPS_BASE_PATH: String = null
  var LOCAL_PATH: String = null
  var HDFS_PATH: String = null
  var HDFS_TEMP_PATH: String = null
  val CONFIGURATION_PATH: String = "/home/plat/configuration.xml"
  val CONFIGURATION_FILENAME: String = "configuration.xml"
  val IVRS_FILES_PATH: String = "/user/plat/files/ivrsData"

  /** Utils **/
  val MAGIC_REGEX = "([^a-zA-Z0-9']+)'*\\1*"

  /** oozie properties **/
  //  val WF: String = "\"wf-"
  //  var DEFAULT_QUEUE_NAME: String = null
  //
  //  /** oozie filenames **/
  //  val COORDINATOR_RES_FILE_NAME: String = "/coordinator"
  //  val COORDINATOR_FILE_NAME: String = "coordinator.xml"
  //  val WORKFLOW_RES_FILE_NAME: String = "/workflow"
  //  val WORKFLOW_FILE_NAME: String = "workflow.xml"
  //  val PROPERTIES_FILE_NAME: String = "job.properties"
  //
  //  /** oozie file headers, main class **/
  //  val COORDINATOR_FILE_HEADER: String = "<coordinator-app name="
  //  val WORKFLOW_FILE_HEADER: String = "<workflow-app name="
  //  val MAIN_CLASS_TAG: String = "<main-class>"
  var MAIN_CLASS_NAME: String = null
  //  var DRIVER_CLASS_NAME: String = null
  //  var OOZIE_DRIVER_NAME: String = "ooziedriver"
  //  var OOZIE_DRIVER_CLASS_NAME: String = "com.platalytics.platform.oozie.OozieDriver"
  //
  //  /** platform jar names and location **/
  //  val JARS_FOLDER: String = "/lib"
  //  val LOGS_FOLDER: String = "/log"
  val OUTPUT_FOLDER: String = "/output"
  //  val BADVALUES_FOLDER: String = "/bad_values"

  //** db types for RDBMS Connector **//
  val DB_ORACLE11G = "oracle11g"
  val DB_POSTGRES = "postgre"
  val DB_MYSQL = "mysql"

  //** db drivers for RDBMS Connector **//
  val DRIVER_POSTGRES = "org.postgresql.Driver"
  val DRIVER_ORACLE11G = "oracle.jdbc.driver.OracleDriver"
  val DRIVER_MYSQL = "com.mysql.jdbc.Driver"

  /**
   * Kraken Schema
   */
    /** misc **/
  val FILTER_STRING: String = "nan~`~NaN~`~nAn~`~NAN~`~\\N~`~null~`~NULL~`~?"
  val SEPARATOR: String = 997.toChar.toString
  val CHAR_SEPARATOR: Char = 997.toChar
  val URL_SEPARATOR: String = "?"
  var FILE_SEPARATOR: String = "/"
  val LINE_SEPARATOR: String = 887.toChar.toString
  val ACTOR_NAME_SEPARATOR: String = "-"
  val SAMPLE_COUNT: Int = 11
  val STATISTICS_SAMPLE_COUNT: Int = 10001
  val TYPE_INFERENCE_SAMPLE_COUNT: Int = 501
  val SAMPLE_FRACTION: Double = 0.2
  var TEMP_MODE_SAMPLE: Int = 101
  val COLUMN_FIELD: String = "field"
  val COLUMN_ALIAS: String = "alias"
  val COLUMN_NAME_KEY: String = COLUMN_ALIAS
  val TEST_MODE: Byte = 0
  val NORMAL_MODE: Byte = 1
  val METER_TO_KM_CONVERSION: Int = 1000
  val METER_TO_MILES_CONVERSION: Double = 1609.34

  /**
   * AWS Regions
   */
  val AP_NORTHEAST_1 = "AP_NORTHEAST_1"
  val AP_NORTHEAST_2 = "AP_NORTHEAST_2"
  val AP_SOUTH_1 = "AP_SOUTH_1"
  val AP_SOUTHEAST_1 = "AP_SOUTHEAST_1"
  val AP_SOUTHEAST_2 = "AP_SOUTHEAST_2"
  val CN_NORTH_1 = "CN_NORTH_1"
  val EU_CENTRAL_1 = "EU_CENTRAL_1"
  val EU_WEST_1 = "EU_WEST_1"
  val SA_EAST_1 = "SA_EAST_1"
  val US_EAST_1 = "US_EAST_1"
  val US_EAST_2 = "US_EAST_2"
  val US_WEST_1 = "US_WEST_1"
  val US_WEST_2 = "US_WEST_2"

  /**
   * Content-Types
   */

  val JSON_CONTENT: String = "application/json"
  val TEXT_CONTENT: String = "text/plain"

  /** Tag types **/
  val FIXED: String = "Fixed"
  val NOMINAL: String = "Nominal"
  val RANGE: String = "Range"
  val LOCATION: String = "Location"

  /** Aggregation types **/
  val AGG_MIN: String = "Min"
  val AGG_MAX: String = "Max"
  val AGG_AVG: String = "Average"
  val AGG_SUM: String = "Sum"
  val AGG_COUNT: String = "Count"

  /** encryption algorithms **/
  val AES: String = "AES"
  val DES: String = "DES"
  val MD5: String = "MD5"
  val SHA256: String = "SHA256"
  val SHA512: String = "SHA512"
  val Blowfish: String = "BLOWFISH"
  val HUFFMAN = "Huffman"
  val RUNLENGTH = "Run Length Encoding"
  val LZW = "Lempel-Ziv-Welch"
  val FIXEDLENGTH = "Fixed-length code"
  val DEFLATER = "Deflater"

  /** data field types **/
  val INT: String = "int"
  val INT16: String = "int16"
  val INT32: String = "int32"
  val INTEGER: String = "integer"
  val INTEGER_TYPE: String = "integertype"
  val DOUBLE: String = "double"
  val DOUBLE_TYPE: String = "doubletype"
  val LATITUDE: String = "latitude"
  val LONGITUDE: String = "longitude"
  val LONG: String = "long"
  val LONG_TYPE: String = "longtype"
  val BIGINT: String = "bigint"
  val FLOAT: String = "float"
  val FLOAT_TYPE: String = "floattype"
  val BYTE: String = "byte"
  val BYTE_TYPE: String = "bytetype"
  val SHORT: String = "short"
  val SHORT_Type: String = "shorttype"
  val BOOLEAN: String = "boolean"
  val BOOLEAN_TYPE: String = "booleantype"
  val STRING: String = "string"
  val STRING_TYPE: String = "stringtype"
  val TEXT: String = "text"
  val VARCHAR: String = "varchar"
  val BLOB: String = "blob"
  val DATETIME: String = "datetime"
  val JSON: String = "json"
  val Geometry: String = "geometry"
  val GeometryUDT: String = "geometrytypeudt"
  val GeometryType: String = "geometrytype"
  val GeometryWKT: String = "geometry(wkt)"
  val GeometryGeoJson: String = "geometry(geojson)"
  val TIMESTAMP: String = "timestamp"
  val TIMESTAMP_TYPE: String = "timestamptype"
  val TIMESTAMP_FORMATTED: String = "timestamp (formatted)"
  val TIMESTAMP_MILLIS: String = "timestamp (milliseconds)"
  val DATE: String = "date"
  val DATE_TYPE: String = "datetype"

  /** staging sink **/
  var STAGING_SINK_PATH: String = null

  /** analytics **/
  var ANALYTICS_PATH: String = null
  var CPR_SCHEMA: String = null
  var VERSION_1_1: String = null
  var ANALYTICS_STATS: String = null
  var DATA_DISCOVERY: String = null
  var SAMPLE_NAME: String = null
  val TRAIN_MODEL: String = "training"
  val TEST_MODEL: String = "testing"
  val MODEL_PREDICTOR: String = "model_predictor_analytics"
  val MONTE_CARLO_ANALYTICS: String = "monte_carlo_analytics"
  val MONTE_CARLO_PREDICTOR: String = "monte_carlo_predictor_analytics"
  val H2OKMEANS_TRAIN: String = "training_sparklingwater_analytics"
  val DECISION_TREE: String = "decisiontree"
  val GRADIENT_BOOSTED_TREES: String = "gradientboostedtrees"
  val RANDOM_FOREST: String = "randomforest"
  val LOGISTIC_REGRESSION_SGD: String = "logisticregressionsgd"
  val SVM_SGD: String = "svmsgd"
  val LINEAR_REGRESSION_SGD: String = "linearregressionsgd"
  val LASSO_REGRESSION_SGD: String = "lassoregressionsgd"
  val RIDGE_REGRESSION_SGD: String = "ridgeregressionsgd"
  val NAIVE_BAYES: String = "naivebayes"
  val KMEANSMLLIB: String = "kmeansmllib"
  val KALMAN_FILTER: String = "kalman_filter_analytics"
  val GAUSSIANMIXTURE: String = "gaussianmixture"
  val MATRIX_FACTORIZATION_MODEL: String = "MatrixFactorizationModel"
  val ALS: String = "alternatingleastsquares"
  val NGRAM: String = "ngram"

  val DECISION_TREE_TRAIN: String = "training_decisiontree_analytics"
  val DECISION_TREE_PREDICT: String = "predicting_decisiontree_analytics"
  val GRADIENT_BOOSTED_TREES_TRAIN: String = "training_gradientboostedtrees_analytics"
  val GRADIENT_BOOSTED_TREES_PREDICT: String = "predicting_gradientboostedtrees_analytics"
  val RANDOM_FOREST_TRAIN: String = "training_randomforest_analytics"
  val RANDOM_FOREST_PREDICT: String = "predicting_randomforest_analytics"
  val LOGISTIC_REGRESSION_SGD_TRAIN: String = "training_logisticregressionsgd_analytics"
  val LOGISTIC_REGRESSION_SGD_PREDICT: String = "predicting_logisticregressionsgd_analytics"
  val SVM_SGD_TRAIN: String = "training_svmsgd_analytics"
  val SVM_SGD_PREDICT: String = "predicting_svmsgd_analytics"
  val LINEAR_REGRESSION_SGD_TRAIN: String = "training_linearregressionsgd_analytics"
  val LINEAR_REGRESSION_SGD_PREDICT: String = "predicting_linearregressionsgd_analytics"
  val LASSO_REGRESSION_SGD_TRAIN: String = "training_lassoregressionsgd_analytics"
  val LASSO_REGRESSION_SGD_PREDICT: String = "predicting_lassoregressionsgd_analytics"
  val RIDGE_REGRESSION_SGD_TRAIN: String = "training_ridgeregressionsgd_analytics"
  val RIDGE_REGRESSION_SGD_PREDICT: String = "predicting_ridgeregressionsgd_analytics"
  val NAIVE_BAYES_TRAIN: String = "training_naivebayes_analytics"
  val NAIVE_BAYES_PREDICT: String = "predicting_naivebayes_analytics"
  val KMEANSMLLIB_TRAIN: String = "training_kmeansmllib_analytics"
  val KMEANSMLLIB_PREDICT: String = "predicting_kmeansmllib_analytics"
  val STREAMING_KMEANS_PREDICT: String = "predicting_streamingkmeans_analytics"
  val GAUSSIANMIXTURE_TRAIN: String = "training_gaussianmixture_analytics"
  val GAUSSIANMIXTURE_PREDICT: String = "predicting_gaussianmixture_analytics"
  val ALS_TRAIN: String = "training_alternatingleastsquares_analytics"
  val ALS_PREDICT: String = "predicting_alternatingleastsquares_analytics"

  val ENSEMBLER_TRAIN: String = "training_ensembler_analytics"
  val ENSEMBLER_PREDICT: String = "predicting_ensembler_analytics"

  val LDA: String = "training_ldamodel_analytics"

  val FPM: String = "frequent_pattern_mining_analytics"
  val ITEMSETS: String = "itemsets"
  val ASSOCIATION_RULES: String = "association_rules"
  val SUBSEQUENCE: String = "subsequence"

  val EXTERNAL_PREDICT: String = "external_predictor_analytics"
  val INTERNAL_PREDICT: String = "internal_predictor_analytics"

  val SENTIMENT_PREDICTOR: String = "sentiment_predictor_analytics"
  val NGRAM_TRAIN: String = "training_ngram_analytics"
  val NGRAM_TEST: String = "predicting_ngram_analytics"
  val NER_TEST: String = "ner_analytics"
  val NER_TAGS: String = "Tags"

  val STRING_INDEXER: String = "String Indexer"
  val BINARIZER: String = "Binarizer"
  val BUCKETIZER: String = "Bucketizer"
  val ONE_HOT_ENCODING: String = "One Hot Encoding"
  val NORMALIZER: String = "Normalizer"
  val STANDARD_SCALAR: String = "Standard Scalar"
  val MIN_MAX_SCALAR: String = "Min Max Scalar"
  val POLYNOMIAL_EXPANSION: String = "Polynomial Expansion"
  val DCT: String = "Discrete Cosine Transform"
  val QUANTILE_DISCRETIZER: String = "Quantile Discretizer"
  val ELEMENT_WISE_PRODUCT: String = "Element Wise Product"
  val HASHING_TF: String = "Hashing TF"
  val HASHING_TFIDF: String = "Hashing TF-IDF"
  val FEATURE_HASHING: String = "Feature Hashing"
  val COUNT_VECTORIZER: String = "Count Vectorizer"
  val WORD_2_VEC: String = "Word 2 Vec"

  val CHI_SQUARE: String = "chi_Sq"
  val PCA: String = "PCA"

  val STRING_INDEXED: String = "_StringIndexed"
  val OHE_INDEXER: String = "_oneHotEncodedIndexer"
  val ONE_HOT_ENCODED: String = "_oneHotEncoded"
  val BINARIZED: String = "_Binarized"
  val BUCKETIZED: String = "_Bucketized"
  val NORMALIZED_ASSEMBLER: String = "NAssembler"
  val NORMALIZED: String = "Normalized"
  val S_SCALED_ASSEMBLER: String = "SAssembler"
  val STANDARD_SCALED: String = "StandardScaled"
  val MINMAX_ASSEMBLER: String = "MMAssembler"
  val MIN_MAX_SCALED: String = "MinMaxScaled"
  val POLYNOMIAL_ASSEMBLER: String = "PolynomialAssembler"
  val EXPANDED_POLYNOMIAL: String = "ExpandedPolynomial"
  val DCT_ASSEMBLER: String = "dctAssembler"
  val DCT_OUTPUT: String = "dctOutput"
  val QUANTILE_DISCRETIZED: String = "_QuantileDiscretized"
  val ELEMENT_WISE_PRODUCT_OUTPUT: String = "_ElementWiseProductOutput"
  val FEATURE_HASHED: String = "FeatureHashed"
  val COUNT_VECTORIZED: String = "CountVectorized"
  val WORD_2_VECTORIZED: String = "word2vectorized"
  val TOKENIZER: String = "tokenizer"
  val SWR: String = "stopwordsremover"
  val NGRAMS: String = "ngrams"
  val HASHINGTF: String = "hashingtf"
  val IDF: String = "inversedocumentfrequency"
  val CV: String = "countvectorizer"
  val W2V: String = "word2vec"

  val TOKENS: String = "_tokens"
  val CREATE_TOKENS: String = "create"
  val DEFAULT_STOPWORDS: String = "default"
  val FILTERED_SW: String = "_FilteredStopWords"
  val N_GRAMS: String = "_ngrams"
  val HASHED_TF: String = "_hashedTF"
  val HASHED_TFIDF: String = "_hashedTFIDF"
  val CUSTOM_DICTIONARY: String = "customDictionary"
  val TEXT_ANALYTICS: String = "_textAnalytics"

  val O_AND_T: String = "OandT"
  val NO_O_AND_T: String = "No_OandT"
  val ORIGINAL_SAMPLE: String = "originalSample"
  val TRANSFORMED_SAMPLE: String = "transformedSample"
  val APPLIED_TRANSFORMATIONS: String = "appliedTransformations"

  val BINARY_CLASS_METRICS: String = "binaryClassMetrics"
  val MULTI_CLASS_METRICS: String = "multiClassMetrics"
  val REGRESSION_METRICS: String = "regressionMetrics"
  val CLUSTERING_METRICS: String = "clusteringMetrics"
  val RECOMMENDER_METRICS: String = "recommenderMetrics"
  val LDA_METRICS: String = "LDAMetrics"
  val NO_VALIDATION: String = "noValidation"
  val K_FOLD_SPLITS: String = "k-fold"
  val PERCENTAGE_SPLITS: String = "percentage"
  val FOLD: String = "Fold_"

  val ACCURACY_AND_ERROR: String = "accuracyAndError"
  val CONFUSION_MATRIX: String = "confusionMatrix"
  val LABELS_STATS: String = "labelsStats"
  val MULTI_CLASS_MODEL_STATS: String = "stats"
  val MULTI_CLASS_MODEL_WEIGHTED_STATS: String = "weightedStats"
  val AREA_UNDER_ROC: String = "auROC"
  val AREA_UNDER_PRC: String = "auPRC"
  val PRC: String = "prc"
  val ROC: String = "roc"
  val PRF_BY_THRESHOLD: String = "prfByThreshold"
  val REGRESSION_STATS: String = "regressionStats"
  val CENTROID_METRICS: String = "centroidMetrics"
  val CLUSTER_STATS: String = "clusterStats"
  val PRECISION_AT_K: String = "precisionAtK"
  val MEAN_AVERAGE_PRECISION: String = "map"
  val NDCG: String = "ndcg"
  val LDA_STATS: String = "ldaStats"
  val BEST_PARAMS: String = "bestParams"

  val MODEL: String = "model"
  val RANDOM: String = "random"
  val PARALLEL: String = "parallel"
  val LR: String = "lr"
  val LGR: String = "lgr"
  val SVM: String = "svm"

  val LASSO: String = "lasso"
  val RIDGE: String = "ridge"
  val L1: String = "l1"
  val L2: String = "l2"
  val DT: String = "dt"
  val RF: String = "rf"
  val GBT: String = "gbt"
  val KMEANS: String = "kmeans"
  val GAUSSIAN: String = "gaussian"
  val AUTO: String = "auto"
  val ALL: String = "all"
  val SQRT: String = "sqrt"
  val LOG2: String = "log2"
  val ONETHIRD: String = "onethird"
  val CLASSIFICATION: String = "classification"
  val REGRESSION: String = "regression"
  val CLUSTERING: String = "clustering"
  val GINI: String = "gini"
  val ENTROPY: String = "entropy"
  val VARIANCE: String = "variance"
  val ENCODE_HASH: String = "hash"
  val ENCODE_ONE_HOT: String = "1-hot"
  val ENCODE_INVERSE_ONE_HOT: String = "1-hot-inverse"
  val SQUARED_ERROR: String = "squared error"
  val ABSOLUTE_ERROR: String = "absolute error"
  val LOGLOSS_ERROR: String = "log"

  val PREDICTED_LABEL: String = "Predicted_Label"

  /**Text Analysis Paths **/
  var ACRONYMS: String = null
  var STOP_WORDS: String = null
  var EMOTICONS: String = null
  var SENTI_DICTIONARY: String = null
  var STOP_WORDS_EN: String = null
  var STOP_WORDS_AR: String = null
  /** analytics algorithms **/
  val ALGO_NAIVEBAYES: String = "naivebayes"
  val ALGO_KMEANS: String = "kmeans"
  val ALGO_GAUSSIANMIXTURE: String = "gaussianmixture"
  val ALGO_DECISIONTREE: String = "decisiontree"
  val ALGO_RANDOMFOREST: String = "randomforest"
  val ALGO_GRADIENTBOOSTEDTREES: String = "gradientboostedtrees"
  val ALGO_LINEARREGRESSION: String = "linearregression"
  val ALGO_LOGISTICREGRESSION: String = "logisticregression"
  val ALGO_SUPPORTVECTORMACHINES: String = "supportvectormachines"
  val ALGO_LASSOREGRESSION: String = "lassoregression"
  val ALGO_RIDGEREGRESSION: String = "ridgeregression"

  /** extensions **/
  val INTERCEPTOR_STAGE: String = "interceptor_extension"
  val CUSTOM_OPERATION: String = "custom_operation_extension"
  val CUSTOM_QUERY: String = "custom_query_extension"

  /** smart sink configurations **/
  var PHOENIX_URL: String = _
  var PHOENIX_JDBC_DRIVER: String = "org.apache.phoenix.jdbc.PhoenixDriver"
  var PHOENIX_CONNECTION_STRING: String = null
  var RESULT_SET_SIZE: Int = 1000

  /** kafka properties**/
  var ZOOKEEPER_URL: String = null
  var KAFKA_BROKERS_LIST: String = null
  val KAFKA_GROUP_ID: String = "0123456789"
  val KAFKA_MAX_RETRIES: Int = 10
  val KAFKA_IS_COMPRESS: Boolean = false
  val KAFKA_BATCH_SIZE: Int = 10

  /** execution mode settings **/
  val START_CODE: Int = 101
  val FINISH_CODE: Int = 202
  val ERROR_CODE: Int = 303
  val ACTUAL: String = "actual"
  val TEMPORARY: String = "temp"
  val DEVELOPMENT: String = "development"
  var STATS_ON: Boolean = true
  var MODE: String = ACTUAL
  var SERVER: String = COMPUTE_TYPE
  val ORIGINAL: String = "original"
  val APPLICATION: String = "application"
  var CONTEXT: String = ORIGINAL
  /** notification status **/
  var DRILL_BIT_URL: String = null
  val STARTED: String = "started"
  val COMPLETED: String = "completed"
  val STREAMING: String = "streaming"
  val FAILED: String = "failed"
  val NO_METRICS: String = "No Metrics Available"

  val HDFS_UPLOADFILE_BASEPATH: String = null

  //** Logging **//*
  var ELASTIC_INDEX_NAME: String = _
  val UNIX_EPOCH_TIME_LENGTH: Int = 13
  val LOG_TYPES: Seq[String] = Seq("debug", "error", "fatal", "info", "trace", "warn")

  /** compression types **/
  val UNCOMPRESSED: String = "uncompressed"
  val GZIP: String = "org.apache.hadoop.io.compress.GzipCodec"
  val BZIP2: String = "org.apache.hadoop.io.compress.BZip2Codec"

  /** interceptor **/
  val BASE_PROJECT_NAME: String = "interceptor"
  var RESOURCE_PATH: String = null
  var BASE_PROJECT_PATH: String = null
  var BASE_PROJECT_PACKAGE: String = null
  val BUILD_SCRIPT_PATH: String = "/platalytics/resources/buildjar.sh"
  val INTERCEPTOR_PATH: String = "/platalytics/resources/interceptor"
  val BUNDLE_SCRIPT_NAME: String = "bundleProcess.sh"
  val SCP_SCRIPT_NAME: String = "scpjar.sh"
  val RUN_SCRIPT_NAME: String = "runjar.sh"
  val PACKAGE: String = "package"
  val CUSTOM_PACKAGE: String = "com.platalytics.platform.custom.s"

  /** internet of things **/
  val DEFAULT_MQTT_BROKER = "tcp://104.236.51.246:1883"
  val MQTT_MESSAGE_ENCODING: String = "utf-8"

  val IOT_DEVICE_MONGO_COLLECTION = "iotdevices"
  val IOT_DEVICE_MONGO_DEVICE_KEY = "key"
  val IOT_DEVICE_MONGO_DEPLOYMENT_STATUS_KEY = "active"

  val IOT_DEPLOYMENT_ARDUINO_PATH: String = "/iot/arduino-yun/deploy.sh"
  val IOT_DEPLOYMENT_RASPBERRY_PATH: String = "/iot/raspberry-pi/deploy.sh"
  val IOT_DEPLOYMENT_INTEL_EDISON: String = "/iot/intel-edison/deploy.sh"

  val IOT_MONGO_DB_NAME: String = "test"
  val IOT_MONGO_COLLECTION_NAME_APPLICATION: String = "iotapps"

  var IOT_FRONT_END_DEPLOYMENT_SERVICE: String = ""
  val URL_TIMEOUT_VALUE = 15000 // 15 seconds

  /** external run mode **/
  val INTERNAL_MODE: String = "INTERNAL"
  val EXTERNAL_MODE: String = "EXTERNAL"
  var RUN_MODE: String = INTERNAL_MODE
  val BUNDLE_JAR_PATH: String = "/platalytics/bundle/"
  val KEY_PATH: String = "/platalytics/keys/"
  var HTTP_SERVER_IP: String = "cloud5-server"
  var HTTP_SERVER_PORT: String = "3061"
  var HTTP_SERVER_PATH: String = "/platalytics/bundle/server/"

  /** stageDTO constants **/
  val STAGE_TYPE_BATCH = "batch"
  val STAGE_TYPE_STREAMING = "streaming"
  val STAGE_TYPE_BATCH_STREAMING = "batch-streaming"

  /** test harness constants **/
  val TEST_HARNESS_SOCKET_PORT: Int = 1234

}