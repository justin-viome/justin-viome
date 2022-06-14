--justin.thomson@viome.com
-- one study reporting query to rule them all

-- 0 should be displayed in all cases including when no references are available in a DB table 
-- results include studies not included in study_info.study


with notifications as (
	select iscft.study_id, count(distinct cfr.user_id) as Users_Contacted
	from chat_flow_request cfr
	join ai_info.chat_flow_template cft on cft.id=cfr.chat_flow_template_id
	join study_info.intervention_study_chat_flow_template iscft on iscft.chat_flow_template_id=cft.id
	group by iscft.study_id
), 
stoolkitsamples as (
-- count GI kits updated with 2 weeks of study participation start time 
	select sp.study_id, count(distinct k."kitId") as StoolCount
	from study_info.study_participant sp
	join study_info.participant p on p.participant_id=sp.participant_id  
	join "KIT" k on k."userId"=p.user_id
	join kit_type kt on kt.id = k.kit_type_id --and k.kit_type_id=1 -- 1=GI
	where k.kit_status_id=10 and (k.updated <= sp.start_time) and ((k.updated + INTERVAL '14 day') >= sp.start_time)
	group by sp.study_id
),
studsamples as (
-- get breakdown of study samples by type
	select study_id, count(distinct study_sample_id) filter (where st.sample_type_id=1) as Stool_Samples,
		count(distinct study_sample_id) filter (where st.sample_type_id=2) as Blood_Samples,
		count(distinct study_sample_id) filter (where st.sample_type_id=3) as Saliva_Samples
	from study_info.study_sample s
	join study_info.sample_type st on s.sample_type_id=st.sample_type_id
	where st.sample_type_id in (1,2,3) -- stool, blood, saliva
	group by s.study_id
),
qanda_sif as (
-- study_info.response stores answers from outside viome online
-- public.ANSWER stores answers from viome online
-- output needs to be combination of the two 
-- feature can be used in multiple studies, but response can only be in one
	select sf.study_id, count(distinct sf.feature_id) as Questions_sif, count(a.*) as Answers_sif 
	from study_info.intervention_study_feature sf
	left join study_info.response a on a.feature_id=sf.feature_id and a.study_id=sf.study_id
	group by sf.study_id
),
-- try getting answer count by counting distinct answer values by study,user,question
-- responsible for 8s of run
userdistinctanswers as (
	SELECT 
	sp.study_id, a."userId" as user_id, a."questionId" as question_id, count(distinct a.answer) as answerCount
	from "ANSWER" a
	join study_info.feature f on a."questionId"=f.question_id
	join study_info.intervention_study_feature isf on isf.feature_id=f.feature_id
	join study_info.study_participant sp on isf.study_id=sp.study_id
	join study_info.participant p on p.participant_id=sp.participant_id and p.user_id=a."userId"
	group by sp.study_id, a."userId", a."questionId"
),
qanda_answers as
( 
	select study_id, count(distinct question_id) as Questions_a, sum(answerCount) as Answers_a
	from userdistinctanswers uda
	group by study_id
), 
-- responsible for 9s of run 
v150results as 
(
	select 15 as study_id, 
		count(distinct gi_record_id) as Participants,
		count (distinct gi_sample_id) as StoolSamp,
		count(distinct viome_received_quest_sample_id) as BloodSamp,
		count (distinct si_sample_id) as SalivaSamp
	from 
	(
		select * from v150_samples_time_1 
		union select * from v150_samples_time_2 
		union select * from v150_samples_time_3 
	) x
),
baseinfo as (
	select s.study_id, s.name, s.description, count(distinct sp.participant_id) as Participants
	from study_info.study s
	left join notifications nt on nt.study_id=s.study_id
	left join study_info.study_participant sp on s.study_id=sp.study_id
	left join studsamples samp on samp.study_id=s.study_id
	group by s.study_id
), v150final AS
(
	select 15 as study_id, 'v150', 'Microbiome in AxSpA disease activity' as description, 0, 94,149,150,148,78,7279
), v109p3 AS
(
select null::bigint as studyid, 'v109.3', 'GRM Validation for Japanese Population - pilot' as description, 0,50, 50,50,50, 26,1199
), v109p4 AS
(
	select null::bigint as studyid, 
	'v109.4', 'GRM Validation for Japanese Population - extended study' as description, 0,500, 
	524,--stool,
	524,--blood
	520,--saliva
	28,12662
), v112 AS
(
	select null::bigint as studyid, 'v112', 'Depression/IBS Study with UCLA' as description, 0,240,240,0,0,48,11219
), v128p1 AS
(
	select null::bigint as studyid, 'v128.1', 'Oral Cancer Study with the University of Queensland Australia' as description, 0,335, 0,0,
	335, -- saliva
	12,2369
), v128p234 AS
(
	select null::bigint as studyid, 'v128.234', 'Oral Cancer - US Controls through EDC and Viome Online' as description, 0,873, -- 873 patients. 1175 in Tunji's model set-302 from 128.1 
	0,0,
	873, -- saliva
	0,0
), 
v300_field_data as (
	SELECT record_id, 
              jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(data -> 'Phases')-> 'Steps')->'Fields') AS field_data 
       FROM   study_info.castor_structured_record 
       WHERE  record_id in (SELECT record_id FROM study_info.castor_structured_record WHERE castor_study_id ='9BB99EB3-8221-45DB-83B1-3BA4195606C2')
),
v300_field_data_table as (
  select record_id, field_data ->>'variable_name' as field, field_data->>'value' as value from v300_field_data
), 
v300 AS 
(
	select 19 as study_id, 'v300', 'Study with NYMC to validate our Oral Cancer detection test' as description,
	0, -- users contacted via app
	count(distinct v.record_id) as participants, 
	0,0,0,
	count(distinct v.field) as questions, -- questions
	count(distinct v.value) as answers -- answers 
	from v300_field_data_table v
), outty as 
(
	select s.study_id, s.name, s.description, 
		coalesce(nt.Users_Contacted,0) as Users_Contacted_Via_App,
		s.Participants as Participants,
		(coalesce(samp.Stool_Samples, 0) + coalesce(sampstool.StoolCount, 0)) as Stool_Samples, 
		(coalesce(samp.Blood_Samples, 0)) as Blood_Samples,
		(coalesce(samp.Saliva_Samples,0)) as Saliva_Samples,
		(coalesce(q.Questions_sif, 0) + coalesce(q2.Questions_a, 0)) as Questions, 
		(coalesce(q.Answers_sif, 0) + coalesce(q2.Answers_a, 0)) as Answers
	from baseinfo s
	left join notifications nt on nt.study_id=s.study_id
	left join studsamples samp on samp.study_id=s.study_id
	left join stoolkitsamples sampstool on sampstool.study_id=s.study_id
	left join qanda_sif q on q.study_id=s.study_id
	left join qanda_answers q2 on q2.study_id=s.study_id
	
	where s.study_id not in (2, 3, 12, 15, 16, 19) 
	-- exclude v126, v112 sans data, v128-Pilot in study_info, v150 due to hardcoding for read speed, Viome Coaching, v300 due to other logic to include parsed JSON
	
	union select * from v109p3
	union select * from v109p4
	union select * from v112
	union select * from v128p1
	union select * from v128p234
	union select * from v150final
	union select * from v300
	order by 1 asc
)
select name, description, participants, stool_samples, blood_samples, saliva_samples, questions, answers, users_contacted_via_app, study_id from outty
order by name asc;
