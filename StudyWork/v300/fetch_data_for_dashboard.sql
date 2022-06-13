-- justin.thomson@viome.com
-- parse castor json to create reports for v300

/*
initial ask:


screen_eligibility
saliva_collection_date1*

Can be computed to include month/year
labss_date_collection
nymc_oc_status
nymc_oc_type
Needs to be coded from lesions as follows,

If,
Dysplasia
Hyperplasia
Leukoplakia
Erythroplasia
Lichenoid lesions
Actinic Keratosis
Lichenoid reaction, 
AND 
nymc_oc_status= negative 
OPMD-M

If,
Aphthous ulcer/ Canker Sores
Gingival enlargement (side effect)
Lichen planus
Keratosis
Inflammatory reaction
Cheek bites, 
AND 
nymc_oc_status= negative OPMD-B
nymc_oc_hpv
*/

--SELECT * FROM study_info.castor_structured_record WHERE castor_study_id ='9BB99EB3-8221-45DB-83B1-3BA4195606C2'


with field_data as (
	SELECT record_id, 
              jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(data -> 'Phases')-> 'Steps')->'Fields') AS field_data 
       FROM   study_info.castor_structured_record 
       WHERE  record_id in (SELECT record_id FROM study_info.castor_structured_record WHERE castor_study_id ='9BB99EB3-8221-45DB-83B1-3BA4195606C2')
),
field_data_table as (
  select record_id, field_data ->>'variable_name' as field, field_data->>'value' as value from field_data
)
SELECT
    record_id,
    MAX(value) FILTER (WHERE field = 'screen_eligibility') AS "screen_eligibility",
    MAX(value) FILTER (WHERE field = 'saliva_collection_date1') AS "saliva_collection_date1",
    MAX(value) FILTER (WHERE field = 'labss_date_collection') AS "labss_date_collection",
    MAX(value) FILTER (WHERE field = 'nymc_oc_status') AS "nymc_oc_status",
    MAX(value) FILTER (WHERE field = 'nymc_oc_type') AS "nymc_oc_type",
    MAX(value) FILTER (WHERE field = 'nymc_oc_hpv') AS "nymc_oc_hpv"
FROM field_data_table
GROUP BY record_id;

    
