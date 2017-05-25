package sparksql
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import Algorithm._
import scala.collection.mutable.MutableList
import scala.Range
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{Buffer,Set,Map}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import Algorithm._
import org.apache.spark.sql.functions._

object tracing {
  

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    Logger.getLogger("hive").setLevel(Level.WARN);
    Logger.getLogger("parse").setLevel(Level.ERROR);

    //    require(args.length == 3)

    val conf = new SparkConf().setAppName("compare_2time")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
   
    val startTime = System.currentTimeMillis(); 
    
    
    hc.sql(s"set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat" +
           s"set mapred.max.split.size=10240000000" +
           s"set mapred.min.split.size.per.node=10240000000" +
           s"set mapred.min.split.size.per.rack=10240000000" +
           s"set mapreduce.jobtracker.split.metainfo.maxsize = -1" +
           s"set mapreduce.job.queuename=root.queue2")
    
   //生成加密后的黑名单卡号表fraudCardsmd5_testA
   hc.sql(s"create table if not exists fraudCards_testA(card string)")
   hc.sql(s"load data local inpath 'fraudCards_testA.txt' into table fraudCards_testA")
   hc.sql(s"create table if not exists fraudCardsmd5_testA(card string)")
   
   hc.sql(s"INSERT OVERWRITE TABLE fraudCardsmd5_testA " +
          s"select arlab.hmacmd5(reverse(concat(length(card),card))) from fraudCards_testA")   //select card from fraudCards_testA;
   hc.sql(s"drop table fraudCards_testA")
   
   //创建可疑交易列表
 
   hc.sql(s"CREATE TABLE IF NOT EXISTS tele_fraud_testA( " +
		s"pri_key                 string, "+   
		s"log_cd                  string, "+   
		s"settle_tp               string, "+   
		s"settle_cycle            string, "+   
		s"block_id                string, "+   
		s"orig_key                string, "+   
		s"related_key             string, "+   
		s"trans_fwd_st            string, "+   
		s"trans_rcv_st            string, "+   
		s"sms_dms_conv_in         string, "+   
		s"fee_in                  string, "+   
		s"cross_dist_in           string, "+   
		s"orig_acpt_sdms_in       string, "+   
		s"tfr_in_in               string, "+   
		s"trans_md                string, "+   
		s"source_region_cd        string, "+   
		s"dest_region_cd          string, "+   
		s"cups_card_in            string, "+   
		s"cups_sig_card_in        string, "+   
		s"card_class              string, "+   
		s"card_attr               string, "+   
		s"sti_in                  string, "+   
		s"trans_proc_in           string, "+   
		s"acq_ins_id_cd           string, "+   
		s"acq_ins_tp              string, "+   
		s"fwd_ins_id_cd           string, "+   
		s"fwd_ins_tp              string, "+   
		s"rcv_ins_id_cd           string, "+   
		s"rcv_ins_tp              string, "+   
		s"iss_ins_id_cd           string, "+   
		s"iss_ins_tp              string, "+   
		s"related_ins_id_cd       string, "+   
		s"related_ins_tp          string, "+   
		s"acpt_ins_id_cd          string, "+   
		s"acpt_ins_tp             string, "+   
		s"pri_acct_no             string, "+   
		s"pri_acct_no_conv        string, "+   
		s"sys_tra_no              string, "+   
		s"sys_tra_no_conv         string, "+   
		s"sw_sys_tra_no           string, "+   
		s"auth_dt                 string, "+   
		s"auth_id_resp_cd         string, "+   
		s"resp_cd1                string, "+   
		s"resp_cd2                string, "+   
		s"resp_cd3                string, "+   
		s"resp_cd4                string, "+   
		s"cu_trans_st             string, "+   
		s"sti_takeout_in          string, "+   
		s"trans_id                string, "+   
		s"trans_tp                string, "+   
		s"trans_chnl              string, "+   
		s"card_media              string, "+   
		s"card_media_proc_md      string, "+   
		s"card_brand              string, "+   
		s"expire_seg              string, "+   
		s"trans_id_conv           string, "+   
		s"settle_dt               string, "+   
		s"settle_mon              string, "+   
		s"settle_d                string, "+   
		s"orig_settle_dt          string, "+   
		s"settle_fwd_ins_id_cd    string, "+   
		s"settle_rcv_ins_id_cd    string, "+   
		s"trans_at                string, "+   
		s"orig_trans_at           string, "+   
		s"trans_conv_rt           string, "+   
		s"trans_curr_cd           string, "+   
		s"cdhd_fee_at             string, "+   
		s"cdhd_fee_conv_rt        string, "+   
		s"cdhd_fee_acct_curr_cd   string, "+   
		s"repl_at                 string, "+   
		s"exp_snd_chnl            string, "+   
		s"confirm_exp_chnl        string, "+   
		s"extend_inf              string, "+   
		s"conn_md                 string, "+   
		s"msg_tp                  string, "+   
		s"msg_tp_conv             string, "+   
		s"card_bin                string, "+   
		s"related_card_bin        string, "+   
		s"trans_proc_cd           string, "+   
		s"trans_proc_cd_conv      string, "+   
		s"tfr_dt_tm               string, "+   
		s"loc_trans_tm            string, "+   
		s"loc_trans_dt            string, "+   
		s"conv_dt                 string, "+   
		s"mchnt_tp                string, "+   
		s"pos_entry_md_cd         string, "+   
		s"card_seq                string, "+   
		s"pos_cond_cd             string, "+   
		s"pos_cond_cd_conv        string, "+   
		s"retri_ref_no            string, "+   
		s"term_id                 string, "+   
		s"term_tp                 string, "+   
		s"mchnt_cd                string, "+   
		s"card_accptr_nm_addr     string, "+   
		s"ic_data                 string, "+   
		s"rsn_cd                  string, "+   
		s"addn_pos_inf            string, "+   
		s"orig_msg_tp             string, "+   
		s"orig_msg_tp_conv        string, "+   
		s"orig_sys_tra_no         string, "+   
		s"orig_sys_tra_no_conv    string, "+   
		s"orig_tfr_dt_tm          string, "+   
		s"related_trans_id        string, "+   
		s"related_trans_chnl      string, "+   
		s"orig_trans_id           string, "+   
		s"orig_trans_id_conv      string, "+   
		s"orig_trans_chnl         string, "+   
		s"orig_card_media         string, "+   
		s"orig_card_media_proc_md string, "+   
		s"tfr_in_acct_no          string, "+   
		s"tfr_out_acct_no         string, "+   
		s"cups_resv               string, "+   
		s"ic_flds                 string, "+   
		s"cups_def_fld            string, "+   
		s"spec_settle_in          string, "+   
		s"settle_trans_id         string, "+   
		s"spec_mcc_in             string, "+   
		s"iss_ds_settle_in        string, "+   
		s"acq_ds_settle_in        string, "+   
		s"settle_bmp              string, "+   
		s"upd_in                  string, "+   
		s"exp_rsn_cd              string, "+   
		s"to_ts                   string, "+   
		s"resnd_num               string, "+   
		s"pri_cycle_no            string, "+   
		s"alt_cycle_no            string, "+   
		s"corr_pri_cycle_no       string, "+   
		s"corr_alt_cycle_no       string, "+   
		s"disc_in                 string, "+   
		s"vfy_rslt                string, "+   
		s"vfy_fee_cd              string, "+   
		s"orig_disc_in            string, "+   
		s"orig_disc_curr_cd       string, "+   
		s"fwd_settle_at           string, "+   
		s"rcv_settle_at           string, "+   
		s"fwd_settle_conv_rt      string, "+   
		s"rcv_settle_conv_rt      string, "+   
		s"fwd_settle_curr_cd      string, "+   
		s"rcv_settle_curr_cd      string, "+   
		s"disc_cd                 string, "+   
		s"allot_cd                string, "+   
		s"total_disc_at           string, "+   
		s"fwd_orig_settle_at      string, "+   
		s"rcv_orig_settle_at      string, "+   
		s"vfy_fee_at              string, "+   
		s"sp_mchnt_cd             string, "+   
		s"acct_ins_id_cd          string, "+   
		s"iss_ins_id_cd1          string, "+   
		s"iss_ins_id_cd2          string, "+   
		s"iss_ins_id_cd3          string, "+   
		s"iss_ins_id_cd4          string, "+   
		s"mchnt_ins_id_cd1        string, "+   
		s"mchnt_ins_id_cd2        string, "+   
		s"mchnt_ins_id_cd3        string, "+   
		s"mchnt_ins_id_cd4        string, "+   
		s"term_ins_id_cd1         string, "+   
		s"term_ins_id_cd2         string, "+   
		s"term_ins_id_cd3         string, "+   
		s"term_ins_id_cd4         string, "+   
		s"term_ins_id_cd5         string, "+   
		s"acpt_cret_disc_at       string, "+   
		s"acpt_debt_disc_at       string, "+   
		s"iss1_cret_disc_at       string, "+   
		s"iss1_debt_disc_at       string, "+   
		s"iss2_cret_disc_at       string, "+   
		s"iss2_debt_disc_at       string, "+   
		s"iss3_cret_disc_at       string, "+   
		s"iss3_debt_disc_at       string, "+   
		s"iss4_cret_disc_at       string, "+   
		s"iss4_debt_disc_at       string, "+   
		s"mchnt1_cret_disc_at     string, "+   
		s"mchnt1_debt_disc_at     string, "+   
		s"mchnt2_cret_disc_at     string, "+   
		s"mchnt2_debt_disc_at     string, "+   
		s"mchnt3_cret_disc_at     string, "+   
		s"mchnt3_debt_disc_at     string, "+   
		s"mchnt4_cret_disc_at     string, "+   
		s"mchnt4_debt_disc_at     string, "+   
		s"term1_cret_disc_at      string, "+   
		s"term1_debt_disc_at      string, "+   
		s"term2_cret_disc_at      string, "+   
		s"term2_debt_disc_at      string, "+   
		s"term3_cret_disc_at      string, "+   
		s"term3_debt_disc_at      string, "+   
		s"term4_cret_disc_at      string, "+   
		s"term4_debt_disc_at      string, "+   
		s"term5_cret_disc_at      string, "+   
		s"term5_debt_disc_at      string, "+   
		s"pay_in                  string, "+   
		s"exp_id                  string, "+   
		s"vou_in                  string, "+   
		s"orig_log_cd             string, "+   
		s"related_log_cd          string, "+   
		s"rec_upd_ts              string, "+   
		s"rec_crt_ts              string, "+   
		s"trans_media             string, "+   
		s"ext_conn_in             string, "+   
		s"ext_cross_region_in     string, "+   
		s"ext_auth_dt             string, "+   
		s"ext_trans_st            string, "+   
		s"ext_trans_attr          string, "+   
		s"ext_trans_at            string, "+   
		s"ext_transmsn_dt_tm      string, "+   
		s"ext_orig_transmsn_dt_tm string, "+   
		s"ext_card_accptr_nm_loc  string, "+   
		s"ext_card_bin            string, "+   
		s"ext_card_bin_id         string, "+   
		s"ext_src_busi_key        string, "+   
		s"ext_card_brand_cd       string, "+   
		s"ext_card_attr_cd        string, "+   
		s"ext_card_prod_cd        string, "+   
		s"ext_card_media_proc_md  string, "+   
		s"ext_status_code         string, "+   
		s"ext_cups_resv_testA_5       string, "+   
		s"ext_cups_resv_6_9       string, "+   
		s"ext_cups_resv_testA0_49     string, "+   
		s"ext_cups_resv_50_testA02    string, "+   
		s"ext_busi_dt             string, "+   
	  s"ext_extend_region_in    string, "+   
		s"ext_eci_flag            string, "+   
		s"ext_extend_inf_4_testA2     string, "+   
		s"ext_extend_inf_testA3       string, "+   
		s"ext_extend_inf_testA4_testA9    string, "+   
		s"ext_extend_inf_20       string, "+   
		s"ext_extend_inf_21       string, "+   
		s"ext_extend_inf_22       string, "+   
		s"ext_extend_inf_23_27    string, "+   
		s"ext_extend_inf_28       string, "+   
		s"ext_token_no            string, "+   
		s"ext_extend_inf_51_54    string, "+   
		s"ext_extend_inf_55_56    string, "+   
		s"ext_vfy_fee_cd_testA_35     string, "+   
		s"ext_vfy_fee_cd_36_53    string, "+   
		s"ext_vfy_fee_cd_54_72    string, "+   
		s"ext_vfy_fee_cd_73_74    string, "+   
		s"ext_vfy_fee_cd_75_76    string, "+   
		s"ext_vfy_fee_cd_77_87    string, "+   
		s"ext_total_card_prod     string, "+   
		s"ext_ma_fwd_ins_id_cd    string, "+   
		s"ext_ma_industry_ins_id_cd       string, "+   
		s"ext_ma_ext_trans_tp     string, "+   
		s"ext_acq_ins_id_cd       string, "+   
		s"ext_hce_prod_nm         string, "+   
		s"ext_mchnt_tp            string, "+   
		s"ext_cups_disc_at        string, "+   
		s"ext_iss_disc_at         string, "+   
		s"ext_spec_disc_tp        string, "+   
		s"ext_spec_disc_lvl       string, "+   
		s"ext_hce_prod_in         string, "+   
		s"ext_touch_tp            string, "+   
		s"ext_carrier_tp          string, "+   
		s"ext_nopwd_petty_in      string, "+   
		s"ext_log_id              string, "+   
		s"ext_white_mchnt_in      string, "+   
		s"ext_up_cloud_app_in     string, "+   
		s"pdate                   string")
   
   
   //在tbl_common_his_trans中寻找黑名单卡号发生的所有转账交易(一段时间内)。
   var Alldata =  hc.sql(s"INSERT OVERWRITE TABLE tele_fraud_testA " + 
		s"select t1.* from tbl_common_his_trans t1 " +
		s"left semi join fraudCardsmd5_testA t2 " +
		s"on t1.tfr_in_acct_no=t2.card " +
		s"where t1.pdate>='20161212' and t1.pdate<='20161212' " +
		s"union all " +
		s"select t1.* from tbl_common_his_trans t1 " +
		s"left semi join fraudCardsmd5_testA t2 " +
		s"on t1.tfr_out_acct_no=t2.card " +
		s"where t1.pdate>='20161212' and t1.pdate<='20161212' " +
		s"union all " +
		s"select t1.* from tbl_common_his_trans t1 " +
		s"left semi join fraudCardsmd5_testA t2 " +
		s"on t1.pri_acct_no_conv=t2.card " +
		s"where t1.pdate>='20161212' and t1.pdate<='20161212' ").cache
   
    Alldata.show()
    println( "Count is : " + Alldata.count())
		
    hc.sql(s"drop table fraudCardsmd5_testA")
    hc.sql(s"drop table tele_fraud_testA")
    
  }
   
  
}