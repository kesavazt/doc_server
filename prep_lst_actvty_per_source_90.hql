set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled=true;
set hive.exec.parallel=true;
set hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=256000000;
set hive.merge.size.per.task=256000000;
set tez.queue.name=root.hive;

SET hive.tez.bucket.pruning=true;
SET hive.optimize.sort.dynamic.partition=true;
set hive.tez.container.size=8048;
set hive.tez.java.opts=-Xmx4000m;
set tez.queue.name=root.zain-modern.daily-c;



insert into ods.prep_lst_actvty_per_source_90
SELECT contract_number, subscriber_number,MAX (last_activity_date) last_activity_date,source_name,cast(from_unixtime(unix_timestamp(date_add(date_add(to_date('${var_wdf_dt_execution_year}-${var_wdf_dt_execution_month}-${var_wdf_dt_execution_day}'),1),-1)),'yyyyMMdd') as bigint) FROM (
select tabs_contrno contract_number, suno subscriber_number , from_unixtime(max(unix_timestamp(`timestamp`))) last_activity_date , 'CRM_IN_ADJ' source_name
from ods.crm_in_adj_stg
WHERE ACCOUNT_TYPE = 'MainAccount'AND VOUCH_BASEREFILL in ('TRUE','FALSE') and ADJAMT>0
--and datediff(date_add(date_add(to_date('${var_wdf_dt_execution_year}-${var_wdf_dt_execution_month}-${var_wdf_dt_execution_day}'),1),-1),from_unixtime(unix_timestamp(cast(timestamp_date as string),'yyyyMMdd'))) <=91
group by tabs_contrno, suno
union
select contrno contract_number, subno subscriber_number , from_unixtime(max(unix_timestamp(transdate))) last_activity_date , 'DBM_TAB_IN' source_name
from ods.dbm_tap_in_stg
--WHERE datediff(date_add(date_add(to_date('${var_wdf_dt_execution_year}-${var_wdf_dt_execution_month}-${var_wdf_dt_execution_day}'),1),-1),from_unixtime(unix_timestamp(cast(upddate_date as string),'yyyyMMdd'))) <=91
group by contrno,subno
union
select tabs_contrno contract_number, msisdn subscriber_number , from_unixtime(max(unix_timestamp(triggertime2))) last_activity_date , 'CRM_IN_EVENTS' source_name
from ods.crm_in_events_stg
WHERE ACCOUNT_TYPE = 'MainAccount'
AND charge_type is NOT null
and NOT (charge_type = 'H' -- First HOP
OR (charge_type = 'L' and call_type = '002') -- incoming local voice calls
OR (charge_type = 'S' and call_type = '030') -- incoming SMS
OR (charge_type = 'W' and call_type = '002') )-- incoming local video calls
--and datediff(date_add(date_add(to_date('${var_wdf_dt_execution_year}-${var_wdf_dt_execution_month}-${var_wdf_dt_execution_day}'),1),-1),from_unixtime(unix_timestamp(cast(triggertime2_date as string),'yyyyMMdd'))) <=91
AND NOT ( (tabs_tariff_group = 'VMAIL2' AND call_type = '029' ) OR ( call_type = '030' and calledpartyno = '786') OR (call_type = '029' and calledpartyno = '155' and CHARGE_TYPE = 'L') OR (call_type = '029' and calledpartyno = '973155' and CHARGE_TYPE = 'L'))
group by tabs_contrno,msisdn
union
select contrno contract_number, subno subscriber_number , from_unixtime(max(unix_timestamp(transdate))) last_activity_date , 'PPCALLS' source_name
from ods.ppcalls_stg
WHERE
-- datediff(date_add(date_add(to_date('${var_wdf_dt_execution_year}-${var_wdf_dt_execution_month}-${var_wdf_dt_execution_day}'),1),-1),from_unixtime(unix_timestamp(cast(transdate_date as string),'yyyyMMdd'))) <=91 AND
NOT ( (tariff_group = 'VMAIL2' AND call_type = '029' ) OR ( call_type = '030' and b_subno = '786') OR (call_type = '029' and b_subno = '155' and CHARGETYPE = 'L'))
AND (case when  call_type = '002' and chargetype = 'L' and DURATION <=0 then 0 else 1 end )= 1
--AND (chargetype = 'H' -- First HOP
--OR (chargetype = 'L' and call_type = '002') -- incoming local voice calls
--OR (chargetype = 'S' and call_type = '030') -- incoming SMS
--OR (chargetype = 'W' and call_type = '002') )
group by contrno, subno
union
SELECT a.tabs_contrno contract_number , suno subscriber_number , from_unixtime(max(unix_timestamp(`timestamp`))) last_activity_date , 'ods.crm_in_adj|ods.service_dim' source_name
from ods.crm_in_adj_stg a,
(select distinct offer_id , service_name from ods.service_dim ) b
WHERE
--datediff(date_add(date_add(to_date('${var_wdf_dt_execution_year}-${var_wdf_dt_execution_month}-${var_wdf_dt_execution_day}'),1),-1),from_unixtime(unix_timestamp(cast(timestamp_date as string),'yyyyMMdd'))) <=91 AND
account_type = 'MainAccount'
AND a.AFT_OFFIDENTFIER = b.offer_id and nvl(a.EXTOUTPUT1,'0') <>'UcDuplicate' and ABS(ADJAMT) >0 and vouch_baserefill is null
AND PAMSERVICEID IS NOT NULL
AND ERRORCODE_ID IS NOT NULL
group by a.tabs_contrno,suno
---------------------------------------------------------------------------
union
SELECT contrno contract_number , subno subscriber_number , concat(DATE_ADD('${var_wdf_dt_execution_year}-${var_wdf_dt_execution_month}-${var_wdf_dt_execution_day}',-89),' 23:00:00') last_activity_date , 'zbhrep_zero_duration_list' source_name
from rpt.zbhrep_zero_duration_list 
group by contrno,subno
----------------------------------------------------------------------------
UNION SELECT contract_number,subscriber_number,last_activity_date,source_name FROM ods.prep_lst_actvty_per_source_90 --last 90 days data accumulated
WHERE snapshot_date = cast (from_unixtime(unix_timestamp(date_add(date_add(to_date('${var_wdf_dt_execution_year}-${var_wdf_dt_execution_month}-${var_wdf_dt_execution_day}'),1),-2)),'yyyyMMdd') as bigint)
and datediff(date_add(date_add(to_date('${var_wdf_dt_execution_year}-${var_wdf_dt_execution_month}-${var_wdf_dt_execution_day}'),1),-1),last_activity_date) < 91
) a
GROUP BY contract_number,subscriber_number,source_name;
