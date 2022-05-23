--justin.thomson@viome.com
-- one study reporting query to rule them all

-- 0 should be displayed in all cases including when no references are available in a DB table 


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
), 	
-- get all questions and answers for v150
survey_json AS 
( 
       SELECT record_id, 
              Jsonb_array_elements(data -> 'Surveys') AS survey_data 
       FROM   study_info.castor_structured_record 
       WHERE  record_id in ('1','100234','100345','100421','100502','100504','100508','100516','100519','100535','100540','100543','100552','100557','100562','100572','100592','100593','100607','100610','100611','100637','100648','100657','100659','100665','100719','100762','100779','100806','100807','100812','100817','100818','100833','100845','100857','100878','100891','100906','100912','100943','100950','100957','100959','100990','101006','101013','101042','101068','101083','101117','101124','101137','101154','101155','101171','101234','101242','101251','101275','101298','101314','101331','101383','101403','101407','101478','101486','101492','101500','101518','101541','101551','101578','101621','101646','101713','101897','101911','102008','110338','110351','200017','200026','200056','200086','200126','200141','3150018','4150034','4150211'))
, 
survey AS 
( 
         SELECT   record_id, 
                  -1 + row_number() over (ORDER BY ( 
                         SELECT 0))             AS survey_instance, 
                  survey_data ->> 'Survey Name' AS survey_name, 
                  survey_data 
         FROM     survey_json 
         --WHERE    (survey_data ->> 'Survey Name' = 'V150 Primary Eligibility Survey') or 
         --	(survey_data ->>'Survey Name' = 'V150 Health Survey')
), 
field_json AS 
( 
       SELECT record_id, 
              survey_instance, 
              survey_name, 
              jsonb_array_elements(jsonb_array_elements(survey_data -> 'Steps') -> 'Fields') AS field_data 
       FROM   survey
), v150qa AS
(
	select 15 as study_id, count(distinct label) as Questions_v150, sum(case when value is null then 0 else 1 end) as Answers_v150
	FROM   field_json, 
	       jsonb_to_record(field_data) AS x( value text, label text)
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
select null::bigint as studyid, 'v112', 'Depression/IBS Study with UCLA' as description, 0,240, 0,0,0,48,11219
)

select s.study_id, s.name, s.description, 
	coalesce(nt.Users_Contacted,0) as Users_Contacted,
	s.Participants + coalesce(v150.Participants, 0) as Participants,
	(coalesce(samp.Stool_Samples, 0) + coalesce(sampstool.StoolCount, 0) + coalesce(v150.StoolSamp, 0)) as Stool_Samples, 
	(coalesce(samp.Blood_Samples, 0)+ coalesce(v150.BloodSamp, 0)) as Blood_Samples,
	(coalesce(samp.Saliva_Samples,0) + coalesce(v150.SalivaSamp, 0)) as Saliva_Samples,
	(coalesce(q.Questions_sif, 0) + coalesce(q2.Questions_a, 0) + coalesce(v150qa.Questions_v150,0)) as Questions, 
	(coalesce(q.Answers_sif, 0) + coalesce(q2.Answers_a, 0) + coalesce(v150qa.Answers_v150, 0)) as Answers
from baseinfo s
left join notifications nt on nt.study_id=s.study_id
left join studsamples samp on samp.study_id=s.study_id
left join stoolkitsamples sampstool on sampstool.study_id=s.study_id
left join qanda_sif q on q.study_id=s.study_id
left join qanda_answers q2 on q2.study_id=s.study_id
left join v150results v150 on v150.study_id= s.study_id
left join v150qa on v150qa.study_id=s.study_id
where s.study_id not in (2, 16) -- v126, Viome Coaching 

union select * from v109p3
union select * from v109p4
union select * from v112
order by 1 asc;

