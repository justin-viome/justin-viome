with field_data as (
	SELECT record_id, 
              jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(data -> 'Phases')-> 'Steps')->'Fields') AS field_data 
       FROM   study_info.castor_structured_record 
       WHERE  record_id in (SELECT record_id FROM study_info.castor_structured_record WHERE castor_study_id ='9BB99EB3-8221-45DB-83B1-3BA4195606C2')
),
field_data_table as (
  select record_id, field_data ->>'variable_name' as field, field_data ->> 'label' as field_label, field_data ->> 'updated_on' as updated_date, field_data->>'value' as value from field_data
)
select * from field_data_table 