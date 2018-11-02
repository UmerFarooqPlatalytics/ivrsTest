package test.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row

object RmSpike {

  val sparkSession = SparkSession.builder
    .master("local[*]").appName("test").getOrCreate

  val query = """
    
    SELECT p.project_no,

                                p.applicant_status,

                                p.dispo,

                                p.country as pt_country,

                                cast(extract(day from p.dob) AS DECIMAL (2,0)) as acr_dob_day,

                                cast(extract(month from p.dob) AS DECIMAL (2,0)) as acr_dob_month,

                                cast(extract(year from p.dob) AS DECIMAL (4,0)) as acr_dob_year,

                                p.first_name as pt_first_name,

                                p.middle as pt_middle_name,

                                p.last_name as pt_last_name,

                                p.gender as pt_gender,

                                p.screening_id,

                                p.site_no,

                                pt.*,

                                ss.facility_cd,

                                i.first_name as inv_first_name,

                                i.middle_name_1 as inv_middle_name,

                                i.last_name as inv_last_name,

                                i.suffix as inv_suffix,

                                rp1.referred_protocols

                FROM acutrack_patient p,

                                s_study.study s,

                                s_site.study_site ss,

                                s_site.investigator i,

                                (

                                                SELECT jppd.protocol_number as acr_protocol_number,

                                                                jppd.protocol_id as acutrack_protocol_id,

                                                                pt1.patient_id as acr_patient_id,

                                                                REPLACE(pt1.screen_failure_comments, CHR (13) || CHR (10), '') as screen_fail_comments,

                                                                cast((case when pt1.appointment_resolution_id = 3 then 1 else 0 end) AS DECIMAL (1,0))as fov,

                                                                cast((case when jppd.consented = -1 then 1 else 0 end)AS DECIMAL (1,0)) as consent,

                                                                cast((case when jppd.enrolled = -1 then 1 else 0 end)AS DECIMAL (1,0)) as enroll,

                                                                cast((case when jppd.randomized = -1 then 1 else 0 end) AS DECIMAL (1,0)) as rand,

                                                                cast((case when pt1.resolved = -1 then 1 else 0 end) AS DECIMAL (1,0)) as resolved,

                                                                coalesce(rel.release_date, ssrc.referred_date) as release_dt,

                                                                pt1.appointment_dt as fov_date,

                                                                jppd.consented_dt as consent_dt,

                                                                jppd.enrolled_dt as enroll_dt,

                                                                jppd.randomized_dt as rand_dt,

                                                                pt1.resolved_dt as resolved_dt

                                                FROM s_acutrack.patient_tracking pt1,

                                                                study_site_referral_count ssrc,

                                                                s_patient.patient rel,

                                                                (

                                                                                SELECT prot.protocol_number,

                                                                                                t.patient_id,

                                                                                                t.protocol_id,

                                                                                                t.evaluated,

                                                                                                t.consented,

                                                                                                t.consented_dt,

                                                                                                t.enrolled,

                                                                                                t.enrolled_dt,

                                                                                                t.randomized,

                                                                                                t.randomized_dt,

                                                                                                t.completed,

                                                                                                t.completed_dt

                                                                                FROM s_acutrack.j_patient_protocol_details t,

                                                                                                s_acutrack.protocol prot

                                                                                WHERE t.consented = -1

                                                                                                and t.protocol_id = prot.protocol_id

                                                                                                and prot.protocol_number in ('JZP166_201' , 'M16_006' , 'M15_991' , 'M14_431' , 'M14_433' , 'EFC14822' , 'AXS_05_301' , 'MVT_601_3001' , 'MVT_601_3002' , 'MVT_601_3101' , 'MVT_601_3102' , '810P301' , '810P302' , 'MVT_601_3003' , 'M15_925' , 'MVT_601_3004' , '822' , 'M16_006' , 'M15_925' , 'M15_991' , 'RIVAROXHFA3001_N' , 'SHP465_305' , 'E2609_G000_202' , 'I1F_MC_RHBV' , 'I1F_MC_RHBW' , 'I1F_MC_RHBX' , 'PHI200808' , 'PHI200808_R' , 'M13_542' , 'M13_545' , 'M13_549' , 'M14_465' , 'M15_555' , 'M13_542' , 'M13_545' , 'M13_549' , 'M14_465' , 'M15_555' , 'B7601003' , 'B7601011' , 'M16_100' , 'M16_100' , 'R475_PN_1523' , 'TV48125_CNS_30056' , 'TV48125_CNS_30057' , 'TV48125_CNS_30056' , 'TV48125_CNS_30057' , 'I1F_MC_RHCD' , 'M14_234' , 'M16_067' , 'I6T_MC_AMAG' , 'RF_I6T_MC_AMAG' , 'I6T_MC_AMAG' , 'RF_I6T_MC_AMAG' , 'E2006_G000_202' , 'APD371_004' , 'APD334_003' , 'VMDN_003' , 'E303' , 'E304' , 'E303' , 'E304' , 'CBYM338E2202' , 'SHP647_301' , 'SHP647_302' , 'G201002' , 'MDCO_PCS_17_04' , 'R475_OA_1611' , 'R475_OA_1688' , 'R475_OA_1688_A' , 'M16_098' , 'M16_098' , '205715' , 'MVT_601_3001' , 'MVT_601_3002' , 'MVT_601_3101' , 'MVT_601_3102' , 'MRS_TU_2019' , 'NYX_2925_2001' , 'I5Q_MC_CGAW' , '551_1155' , 'K_877_302' , 'CLR_11_03' , 'AE051_G_13_003' , 'XmAb5871_04' , 'SB_FIX_1501' , 'M14_431' , 'M14_433' , 'M16_067' , 'KPL_716_C001' , '42847922MDD2002' )

                                                                ) jppd,

                                                                (

                                                                                select p.patient_id

                                                                                from s_acutrack.patient p,

                                                                                                s_study.study s,

                                                                                                s_site.study_site ss

                                                                                where p.acuscreen_study_id = s.study_id

                                                                                                and p.acuscreen_study_id = ss.study_id

                                                                                                and p.site_no = ss.site_num

                                                                                                and (ss.test_site_ind != 'Y'

                                                                                                                or ss.test_site_ind is null)

                                    and p.project_no in('4579' , '3889' , '3889' , '3889' , '3889' , '4241' , '3159' , '4384' , '4384' , '4385' , '4385' , '3532' , '3532' , '4384' , '2821' , '4384' , '4241' , '3889OUS' , '2821ROW' , '3889OUS' , '2433N' , '2578' , '2731' , '2774' , '2774' , '2774' , '2801' , '2801' , '2821' , '2821' , '2821' , '2821' , '2821' , '2821ROW' , '2821ROW' , '2821ROW' , '2821ROW' , '2821ROW' , '2823' , '2823' , '3017' , '3017X' , '3138' , '3237' , '3237' , '3237OUS' , '3237OUS' , '3262' , '3264' , '3264' , '3485' , '3485' , '3485OUS' , '3485OUS' , '3634' , '3638C' , '3638UC' , '3769' , '3792' , '3792' , '3792OUS' , '3792OUS' , '3793' , '3839' , '3839' , '3923' , '3962' , '4109' , '4109' , '4109A' , '4319' , '4319OUS' , '4337' , '4384OUS' , '4384OUS' , '4385OUS' , '4385OUS' , '4497' , '4557' , '4686' , '47529' , '4763OUS' , '52160' , '58769' , '60544' , '65831' , '3889OUS' , '3889OUS' , '3264OUS' , '4631' , '4840' )

                                                                ) p1

                                                where p1.patient_id = pt1.patient_id

                                                                and pt1.patient_id = jppd.patient_id (+)

                                                                and pt1.patient_id = rel.patient_id (+)

                                                                and pt1.patient_id = ssrc.patient_id (+)

                                                                and coalesce(rel.release_date, ssrc.referred_date) >= sysdate - 720

                                ) pt,

                                (

                                                select f.facility_cd,

                                                                fg.facility_grp_type

                                                from s_site.facility f

                                                left join s_site.facility_group fg

                                                on f.facility_grp_id = fg.facility_grp_id

                                ) fac,

                                (

                                                select p.patient_id,

                                                                listagg(pr.protocol_num, ',') within group (order by protocol_num) as referred_protocols

                                                from s_acutrack.patient p,

                                                                s_patient.patient_protocol_qualify ppq,

                                                                s_site.study_site_protocol ssp,

                                                                s_study.protocol pr

                                                where p.patient_id = ppq.patient_id

                                                                and ppq.protocol_id = ssp.protocol_id

                                                                and p.acuscreen_study_id = ssp.study_id

                                                                and p.site_no = ssp.site_num

                                                                and ssp.protocol_id = pr.protocol_id

                            and p.project_no in ('4579' , '3889' , '3889' , '3889' , '3889' , '4241' , '3159' , '4384' , '4384' , '4385' , '4385' , '3532' , '3532' , '4384' , '2821' , '4384' , '4241' , '3889OUS' , '2821ROW' , '3889OUS' , '2433N' , '2578' , '2731' , '2774' , '2774' , '2774' , '2801' , '2801' , '2821' , '2821' , '2821' , '2821' , '2821' , '2821ROW' , '2821ROW' , '2821ROW' , '2821ROW' , '2821ROW' , '2823' , '2823' , '3017' , '3017X' , '3138' , '3237' , '3237' , '3237OUS' , '3237OUS' , '3262' , '3264' , '3264' , '3485' , '3485' , '3485OUS' , '3485OUS' , '3634' , '3638C' , '3638UC' , '3769' , '3792' , '3792' , '3792OUS' , '3792OUS' , '3793' , '3839' , '3839' , '3923' , '3962' , '4109' , '4109' , '4109A' , '4319' , '4319OUS' , '4337' , '4384OUS' , '4384OUS' , '4385OUS' , '4385OUS' , '4497' , '4557' , '4686' , '47529' , '4763OUS' , '52160' , '58769' , '60544' , '65831' , '3889OUS' , '3889OUS' , '3264OUS' , '4631' , '4840' )

                            and pr.protocol_num in ('JZP166_201' , 'M16_006' , 'M15_991' , 'M14_431' , 'M14_433' , 'EFC14822' , 'AXS_05_301' , 'MVT_601_3001' , 'MVT_601_3002' , 'MVT_601_3101' , 'MVT_601_3102' , '810P301' , '810P302' , 'MVT_601_3003' , 'M15_925' , 'MVT_601_3004' , '822' , 'M16_006' , 'M15_925' , 'M15_991' , 'RIVAROXHFA3001_N' , 'SHP465_305' , 'E2609_G000_202' , 'I1F_MC_RHBV' , 'I1F_MC_RHBW' , 'I1F_MC_RHBX' , 'PHI200808' , 'PHI200808_R' , 'M13_542' , 'M13_545' , 'M13_549' , 'M14_465' , 'M15_555' , 'M13_542' , 'M13_545' , 'M13_549' , 'M14_465' , 'M15_555' , 'B7601003' , 'B7601011' , 'M16_100' , 'M16_100' , 'R475_PN_1523' , 'TV48125_CNS_30056' , 'TV48125_CNS_30057' , 'TV48125_CNS_30056' , 'TV48125_CNS_30057' , 'I1F_MC_RHCD' , 'M14_234' , 'M16_067' , 'I6T_MC_AMAG' , 'RF_I6T_MC_AMAG' , 'I6T_MC_AMAG' , 'RF_I6T_MC_AMAG' , 'E2006_G000_202' , 'APD371_004' , 'APD334_003' , 'VMDN_003' , 'E303' , 'E304' , 'E303' , 'E304' , 'CBYM338E2202' , 'SHP647_301' , 'SHP647_302' , 'G201002' , 'MDCO_PCS_17_04' , 'R475_OA_1611' , 'R475_OA_1688' , 'R475_OA_1688_A' , 'M16_098' , 'M16_098' , '205715' , 'MVT_601_3001' , 'MVT_601_3002' , 'MVT_601_3101' , 'MVT_601_3102' , 'MRS_TU_2019' , 'NYX_2925_2001' , 'I5Q_MC_CGAW' , '551_1155' , 'K_877_302' , 'CLR_11_03' , 'AE051_G_13_003' , 'XmAb5871_04' , 'SB_FIX_1501' , 'M14_431' , 'M14_433' , 'M16_067' , 'KPL_716_C001' , '42847922MDD2002' )

                                                group by p.patient_id

                                ) rp1,

                                (

                                                select p.patient_id

                                                from s_acutrack.patient p,

                                                                s_patient.patient_protocol_qualify ppq,

                                                                s_site.study_site_protocol ssp,

                                                                s_study.protocol pr

                                                where p.patient_id = ppq.patient_id

                                                                and ppq.protocol_id = ssp.protocol_id

                                                                and p.acuscreen_study_id = ssp.study_id

                                                                and p.site_no = ssp.site_num

                                                                and ssp.protocol_id = pr.protocol_id

                            and p.project_no in('4579' , '3889' , '3889' , '3889' , '3889' , '4241' , '3159' , '4384' , '4384' , '4385' , '4385' , '3532' , '3532' , '4384' , '2821' , '4384' , '4241' , '3889OUS' , '2821ROW' , '3889OUS' , '2433N' , '2578' , '2731' , '2774' , '2774' , '2774' , '2801' , '2801' , '2821' , '2821' , '2821' , '2821' , '2821' , '2821ROW' , '2821ROW' , '2821ROW' , '2821ROW' , '2821ROW' , '2823' , '2823' , '3017' , '3017X' , '3138' , '3237' , '3237' , '3237OUS' , '3237OUS' , '3262' , '3264' , '3264' , '3485' , '3485' , '3485OUS' , '3485OUS' , '3634' , '3638C' , '3638UC' , '3769' , '3792' , '3792' , '3792OUS' , '3792OUS' , '3793' , '3839' , '3839' , '3923' , '3962' , '4109' , '4109' , '4109A' , '4319' , '4319OUS' , '4337' , '4384OUS' , '4384OUS' , '4385OUS' , '4385OUS' , '4497' , '4557' , '4686' , '47529' , '4763OUS' , '52160' , '58769' , '60544' , '65831' , '3889OUS' , '3889OUS' , '3264OUS' , '4631' , '4840'  )

                                                                and pr.protocol_num in ('JZP166_201' , 'M16_006' , 'M15_991' , 'M14_431' , 'M14_433' , 'EFC14822' , 'AXS_05_301' , 'MVT_601_3001' , 'MVT_601_3002' , 'MVT_601_3101' , 'MVT_601_3102' , '810P301' , '810P302' , 'MVT_601_3003' , 'M15_925' , 'MVT_601_3004' , '822' , 'M16_006' , 'M15_925' , 'M15_991' , 'RIVAROXHFA3001_N' , 'SHP465_305' , 'E2609_G000_202' , 'I1F_MC_RHBV' , 'I1F_MC_RHBW' , 'I1F_MC_RHBX' , 'PHI200808' , 'PHI200808_R' , 'M13_542' , 'M13_545' , 'M13_549' , 'M14_465' , 'M15_555' , 'M13_542' , 'M13_545' , 'M13_549' , 'M14_465' , 'M15_555' , 'B7601003' , 'B7601011' , 'M16_100' , 'M16_100' , 'R475_PN_1523' , 'TV48125_CNS_30056' , 'TV48125_CNS_30057' , 'TV48125_CNS_30056' , 'TV48125_CNS_30057' , 'I1F_MC_RHCD' , 'M14_234' , 'M16_067' , 'I6T_MC_AMAG' , 'RF_I6T_MC_AMAG' , 'I6T_MC_AMAG' , 'RF_I6T_MC_AMAG' , 'E2006_G000_202' , 'APD371_004' , 'APD334_003' , 'VMDN_003' , 'E303' , 'E304' , 'E303' , 'E304' , 'CBYM338E2202' , 'SHP647_301' , 'SHP647_302' , 'G201002' , 'MDCO_PCS_17_04' , 'R475_OA_1611' , 'R475_OA_1688' , 'R475_OA_1688_A' , 'M16_098' , 'M16_098' , '205715' , 'MVT_601_3001' , 'MVT_601_3002' , 'MVT_601_3101' , 'MVT_601_3102' , 'MRS_TU_2019' , 'NYX_2925_2001' , 'I5Q_MC_CGAW' , '551_1155' , 'K_877_302' , 'CLR_11_03' , 'AE051_G_13_003' , 'XmAb5871_04' , 'SB_FIX_1501' , 'M14_431' , 'M14_433' , 'M16_067' , 'KPL_716_C001' , '42847922MDD2002' )

                                                group by p.patient_id

                                ) rp2

                WHERE p.patient_id = pt.acr_patient_id

                                and p.acuscreen_study_id = s.study_id

                                and p.project_no in('4579' , '3889' , '3889' , '3889' , '3889' , '4241' , '3159' , '4384' , '4384' , '4385' , '4385' , '3532' , '3532' , '4384' , '2821' , '4384' , '4241' , '3889OUS' , '2821ROW' , '3889OUS' , '2433N' , '2578' , '2731' , '2774' , '2774' , '2774' , '2801' , '2801' , '2821' , '2821' , '2821' , '2821' , '2821' , '2821ROW' , '2821ROW' , '2821ROW' , '2821ROW' , '2821ROW' , '2823' , '2823' , '3017' , '3017X' , '3138' , '3237' , '3237' , '3237OUS' , '3237OUS' , '3262' , '3264' , '3264' , '3485' , '3485' , '3485OUS' , '3485OUS' , '3634' , '3638C' , '3638UC' , '3769' , '3792' , '3792' , '3792OUS' , '3792OUS' , '3793' , '3839' , '3839' , '3923' , '3962' , '4109' , '4109' , '4109A' , '4319' , '4319OUS' , '4337' , '4384OUS' , '4384OUS' , '4385OUS' , '4385OUS' , '4497' , '4557' , '4686' , '47529' , '4763OUS' , '52160' , '58769' , '60544' , '65831' , '3889OUS' , '3889OUS' , '3264OUS' , '4631' , '4840' )

                                and p.applicant_status = 'R'

                                and p.dispo = 1

                                and p.acuscreen_study_id = ss.study_id

                                and p.site_no = ss.site_num

                                and (ss.test_site_ind != 'Y'

                                                or ss.test_site_ind is null)

                                and ss.facility_cd = fac.facility_cd

                                and (lower(fac.facility_grp_type) != 'radiant'

                                                or fac.facility_grp_type is null)

                                and ss.investigator_id = i.investigator_id

                                and p.patient_id = rp1.patient_id

                                and p.patient_id = rp2.patient_id
    
    """

  def main(args: Array[String]) {
    val df = fetch("prd-db-scan.acurian.com", "1521", "S_NUMTRA", "S_NUMTRA#2018", "oracle11g", "acuprd_app_numtra.acurian.com", s"(${query})")
    
    df.printSchema
    
    var writer: DataFrameWriter[Row] = null
    writer = df.write.format("com.databricks.spark.csv")
    writer.option("header", "true")
      .option("delimiter", ",")
      .save("/spikeTest")
      
      
    val writtenData = sparkSession.sqlContext.read
        .format("com.databricks.spark.csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ",")
        .load("/spikeTest")
        
    writtenData.printSchema    
    
  }

  def fetch(host: String, port: String, username: String, password: String, dbType: String, databasename: String, query: String): DataFrame = {
    var prop = new java.util.Properties
    val url = getConnectionString(dbType, username, password, host, port, databasename)
    prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
    prop.setProperty("user", username)
    prop.setProperty("password", password)
    prop.setProperty("allowExisting", "true")
    val queryOrTable = s"(${query})"
    sparkSession.sqlContext.read.jdbc(url, queryOrTable, prop)
  }

  def getConnectionString(dbType: String, userName: String, password: String, host: String, port: String, dbName: String): String = {
    dbType.toLowerCase() match {
      case "mysql"     => s"jdbc:mysql://${host}:${port}/${dbName}?user=${userName}&password=${password}"
      case "postgres"  => s"jdbc:postgresql://${host}:${port}/?user=${userName}&password=${password}"
      case "oracle11g" => s"jdbc:oracle:thin:${userName}/${password}@${host}:${port}/${dbName}"
      case _           => ""
    }
  }

}