
-- using distinct to get counts given participants can appear multiple times for one study in the backend 
select s.study_id, s.name, s.description, count(distinct sp.participant_id) as Participant_Count
from study_info.study s
left join study_info.study_participant sp on s.study_id=sp.study_id
group by s.study_id
order by 1 asc;

-- there any participants that have multiple rows for the same study
select p.study_id, p.participant_id, count(*) 
from study_info.study_participant p 
group by p.study_id, p.participant_id
having count(p.participant_id)>1
order by 2 desc;

-- subjects can switch interventions in a study and then are represented with two rows in the study_participants table 
select * from study_info.study_participant p where p.participant_id=19763 and p.study_id=8;
-- this study had two parts. Some participants decided to proceed 

-- get count of users contacted for each study
select cft.name, cft.description, count(distinct cfr.user_id) as DistinctUsersContacted
from chat_flow_request cfr
join ai_info.chat_flow_template cft on cft.id=cfr.chat_flow_template_id
group by cfr.chat_flow_template_id, cft.name, cft.description
order by 1,2,3;


select * from study_info.study_participant sp
join study_info.participant p on p.participant_id=sp.participant_id  
where study_id=5
order by start_time asc;
-- participantid=893, userid=59268 2019-09-17

-- ask Angel if it's okay if I make lower case views for KIT USER
select * from "KIT" k
join test on test.kit_id = k."kitId"
where k."userId"=59268 and k.kit_status_id=10;

-- 7710 
select count(distinct k."userId") from study_info.study_participant sp
join study_info.participant p on p.participant_id=sp.participant_id  
join "KIT" k on k."userId"=p.user_id
where study_id=5 and k.kit_status_id=10
order by k."userId" desc;

select k."userId", k.updated as kit_updated, k."kitId" as kit_id, sp.start_time as study_participation_start_time, * from study_info.study_participant sp
join study_info.participant p on p.participant_id=sp.participant_id  
join "KIT" k on k."userId"=p.user_id
where study_id=5 and k.kit_status_id=10 and k.kit_type_id=1 -- 1=GI
order by k."userId" desc;


-- to get list of kits for studies, look at kit updated time when kit status is 10 and find only those records that are within 3 weeks of study_particpation start time
-- could also be within 3 weeks of answer to questionid=335, which is when customer says they collected sample 
-- sample collection (self-reported) vs when kit was sequenced at lab 
-- pedro will confirm 

-- 9449 successfully processed kits were updated before study particpation start time 
-- 6426 ofthese were updated within 3 weeks
-- 5633 within 2 weeks 
with kits_for_study_users AS (
select sp.study_id, k."userId" as user_id, k.updated as kit_updated, k."kitId" as kit_id, kt.short_name, sp.start_time as study_participation_start_time
from study_info.study_participant sp
join study_info.participant p on p.participant_id=sp.participant_id  
join "KIT" k on k."userId"=p.user_id
join kit_type kt on kt.id = k.kit_type_id and k.kit_type_id=1 -- 1=GI
where k.kit_status_id=10
)

select k.study_id, count(distinct k.kit_id) from kits_for_study_users k 
where (k.kit_updated <= study_participation_start_time) and ((k.kit_updated + INTERVAL '14 day') >= study_participation_start_time)
group by k.study_id;


-- there's more to kits than kit table. Angel will update me on how they get updated. 

-- can look at study_sample and get count of distinct externalid (bioinfoedge reference)
-- one participant may have more than one 

-- discuss with Pedro and Angel 
-- 4350 samples vs 5633 kits  within 2 weeks 
select * from study_info.study_sample;

-- 20 rows have null study id
select * from study_info.study_sample where study_id is null;

-- get breakdown of study samples by type
select study_id, count(distinct study_sample_id) filter (where st.sample_type_id=1) as "Stool Samples",
	count(distinct study_sample_id) filter (where st.sample_type_id=2) as "Blood Samples",
	count(distinct study_sample_id) filter (where st.sample_type_id=3) as "Saliva Samples"
from study_info.study_sample s
join study_info.sample_type st on s.sample_type_id=st.sample_type_id
where st.sample_type_id in (1,2,3) -- stool, blood, saliva
group by s.study_id
order by s.study_id asc;

-- are any questions answered multiple times? 
-- yes; took 22s to run so commenting out
--select a."userId", a.questionnaire_id, count(*) as answer_count 
--from "ANSWER" a
--group by a."userId", a.test_id
--order by 3 desc;

-- questionnaire_id is always null 
select count(*) from "ANSWER" where questionnaire_id is not null;

-- for questions can join study_info.intervention_study_feature with feature and join with question.  
-- for now considering one feature=one question 
-- can count # of questions by looking just at interventionstudyfeature
select sf.study_id, count(distinct sf.feature_id) as question_count 
from study_info.intervention_study_feature sf
group by study_id
order by study_id;

-- count answers
select sf.study_id, count(a.*) as answer_count
from study_info.response a 
join study_info.intervention_study_feature sf on a.feature_id=sf.feature_id
group by sf.study_id
order by sf.study_id;



-- need to join against ANSWER to get answers, not response

-- add ANSWER count + response count 

-- studyinfo response has data from non-viome online customers
--- chatbot answers end up here
--study_info.intervention_study_feature, feature, answer

-- TODO
-- for stool only
studyid=1,10,11 can use study sample
for every other study need to look in kit table
- can add counts for a general approach


-- Castor playaround
select s.*, css.castor_study_id, 'Phase' top_element 
from study_info.study s
join study_info.castor_study_structure css on css.study_id = s.study_id
order by 1 desc; 

- 5,6,8, 12, 15
-- 12, 15 are in castor
-- studies intended to identify biomarkers 
-- distribution of questionnaire scores could be worth visualizing

-- service that scores questionnaires go off questionnaireid, 


	select study_id, count(distinct study_sample_id) filter (where st.sample_type_id=1) as Stool_Samples,
		count(distinct study_sample_id) filter (where st.sample_type_id=2) as Blood_Samples,
		count(distinct study_sample_id) filter (where st.sample_type_id=3) as Saliva_Samples
	from study_info.study_sample s
	join study_info.sample_type st on s.sample_type_id=st.sample_type_id
	where st.sample_type_id in (1,2,3) -- stool, blood, saliva

select p.user_id, * from study_info.study_participant
join study_info.participant p on p.participant_id=study_participant.participant_id where study_id=9;

	select a."userId", count(distinct a."questionId"), count(distinct a."id") as answers
	from "ANSWER" a 
	join study_info.feature f on a."questionId"=f.question_id
	join study_info.intervention_study_feature isf on isf.feature_id=f.feature_id
	join study_info.study_participant sp on isf.study_id=sp.study_id
	join study_info.participant p on p.participant_id=sp.participant_id and p.user_id=a."userId"
	where a.answerdate > sp.start_time and a.answerdate < sp.end_time and a.test_id is null and sp.study_id=9
	group by a."userId"
	order by 3 desc;
	
	-- latest answer by questionid
	-- ignores cases where one question may have been validly answered > 1 times 
	-- reach out to Oliver/table should be cleaned up and bug removed
with tempt as 
(
	SELECT row_number()
		over (partition by a."userId", a."questionId" ORDER BY answerdate desc) as row_number, 
		a."userId" as user_id, a."questionId" as question_id, a."id" as answer_id, sp.study_id
		FROM "ANSWER" a
		join study_info.feature f on a."questionId"=f.question_id
		join study_info.intervention_study_feature isf on isf.feature_id=f.feature_id
		join study_info.study_participant sp on isf.study_id=sp.study_id
		join study_info.participant p on p.participant_id=sp.participant_id and p.user_id=a."userId"

) 
SELECT a.study_id, count(distinct a.question_id), count(distinct a.answer_id) as answers from tempt a
where a.row_number=1
group by a.study_id
order by 3 desc;

-- try getting answer count by counting distinct answer values by study,user,question
with userdistinctanswers as (
	SELECT 
	sp.study_id, a."userId" as user_id, a."questionId" as question_id, count(distinct a.answer) as answerCount
	from "ANSWER" a
	join study_info.feature f on a."questionId"=f.question_id
	join study_info.intervention_study_feature isf on isf.feature_id=f.feature_id
	join study_info.study_participant sp on isf.study_id=sp.study_id
	join study_info.participant p on p.participant_id=sp.participant_id and p.user_id=a."userId"
	group by sp.study_id, a."userId", a."questionId"
) select study_id, count(distinct question_id) as Questions_a, sum(answerCount) as Answers_a
from userdistinctanswers uda
group by study_id
order by 2;

-- try getting answer count by counting distinct answer values by study,user,question
with userdistinctanswers as (
	SELECT 
	sp.study_id, a."userId" as user_id, a."questionId" as question_id, count(distinct a.answer) as answerCount
	from "ANSWER" a
	join study_info.feature f on a."questionId"=f.question_id
	join study_info.intervention_study_feature isf on isf.feature_id=f.feature_id
	join study_info.study_participant sp on isf.study_id=sp.study_id
	join study_info.participant p on p.participant_id=sp.participant_id and p.user_id=a."userId"
	group by sp.study_id, a."userId", a."questionId"
)
, qanda_ans as
( 
		select study_id, count(distinct question_id) as Questions_a, sum(answerCount) as Answers_a
	from userdistinctanswers uda
	group by study_id
) select * from qanda_ans

		
-- query to highlight write problem with ANSWER table 
select a."userId", a."questionId", count(distinct a."id") as answers
from "ANSWER" a 
group by a."userId", a."questionId"
having count(distinct a."id") > 100
order by 3 desc;

select * from v150_samples_time_2

-- get similar data using v150 views 
select 15 as study_id, 

with v150t1 as 
(
	select 15 as study_id, 
		count(distinct gi_record_id) as Participants,
		count (distinct gi_sample_id) as StoolSamp,
		count(distinct viome_received_quest_sample_id) as BloodSamp,
		count (distinct si_sample_id) as SalivaSamp
	from v150_samples_time_1 v1
),
with v150t2 as 
(
	select 15 as study_id, 
		count(distinct gi_record_id) as Participants,
		count (distinct gi_sample_id) as StoolSamp,
		count(distinct viome_received_quest_sample_id) as BloodSamp,
		count (distinct si_sample_id) as SalivaSamp
	from v150_samples_time_2 v2
),
with v150t3 as 
(
	select 15 as study_id, 
		count(distinct gi_record_id) as Participants,
		count (distinct gi_sample_id) as StoolSamp,
		count(distinct viome_received_quest_sample_id) as BloodSamp,
		count (distinct si_sample_id) as SalivaSamp
	from v150_samples_time_3 v3
)

-- 9.2sto return all. fastest approach.
select * from v150_samples_time_1 -- 3.5
union select * from v150_samples_time_2 -- 3.5
union select * from v150_samples_time_3 -- 3.4

with v150results as 
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
)
select * from v150results;

select count(*) from study_info.castor_structured_record csr
join study_info.castor_study_structure css on csr.castor_study_id=css.castor_study_id
where css.study_id=15;

-- get list of 92 participant castor record_ids
-- get comma-delimited list of strings for qanda v150 query
select distinct string_agg(distinct gi_record_id::text, ''',''') as record_id 	
from 
(
	select * from v150_samples_time_1 
	union select * from v150_samples_time_2 
	union select * from v150_samples_time_3 
) x
where gi_record_id is not null;
	
	
-- get all questions and answers
WITH survey_json AS 
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
	select count(*) as Questions_v150, sum(case when value is null then 0 else 1 end) as Answers_v150
	FROM   field_json, 
	       jsonb_to_record(field_data) AS x( value text)
)
select 15 as study_id, * from v150qa;
