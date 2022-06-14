-- justin.thomson@viome.com
-- parse castor json to create dashboard table for v300

--v300 is castor_study_id ='9BB99EB3-8221-45DB-83B1-3BA4195606C2' is study_id=19.


with field_data as (
	SELECT record_id, 
              jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(data -> 'Phases')-> 'Steps')->'Fields') AS field_data 
       FROM   study_info.castor_structured_record 
       WHERE  record_id in (SELECT record_id FROM study_info.castor_structured_record WHERE castor_study_id ='9BB99EB3-8221-45DB-83B1-3BA4195606C2')
),
field_data_table as (
  select record_id, field_data ->>'variable_name' as field, field_data->>'value' as value from field_data
),
subject_report as (
SELECT
    record_id,
    MAX(value) FILTER (WHERE field = 'screen_eligibility') AS "screen_eligibility",
    MAX(value) FILTER (WHERE field = 'saliva_collection_date1') AS "saliva_collection_date1",
    MAX(value) FILTER (WHERE field = 'labss_date_collection') AS "labss_date_collection",
    MAX(value) FILTER (WHERE field = 'lesions') AS "lesions",
    MAX(value) FILTER (WHERE field = 'nymc_oc_status') AS "nymc_oc_status",
    MAX(value) FILTER (WHERE field = 'nymc_oc_type') AS "nymc_oc_type",
    MAX(value) FILTER (WHERE field = 'nymc_oc_hpv') AS "nymc_oc_hpv"
FROM field_data_table
GROUP BY record_id
)
select record_id as "Castor Patient ID", screen_eligibility as "Screen Eligibility", saliva_collection_date1 as "Date of Saliva Collection", labss_date_collection as "Date Saliva Sample Received", nymc_oc_status as "Oral/Throat Cancer Status", nymc_oc_type as "Type of Oral Cancer", 
case when (nymc_oc_status='Positive' AND lesions in ('test')) then '' else '' end as "OPMD",
nymc_oc_hpv as "HPV Status"
from subject_report
order by 1;
    
