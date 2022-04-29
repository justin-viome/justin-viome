
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
where study_id=5 and k.kit_status_id=10
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
join kit_type kt on kt.id = k.kit_type_id
where study_id=5 and k.kit_status_id=10
)

select * from kits_for_study_users k 
where (k.kit_updated <= study_participation_start_time) and ((k.kit_updated + INTERVAL '14 day') >= study_participation_start_time)
order by k.study_id, k.user_id desc;


-- there's more to kits than kit table. Angel will update me on how they get updated. 

-- can look at study_sample and get count of distinct externalid (bioinfoedge reference)
-- one participant may have more than one 


-- 4350 samples vs 5633 kits  within 2 weeks 
select * from study_info.study_sample;
-- discuss with Pedro and Angel 

-- get number of questions asked 

-- get number of questions answered 

-- are any questions answered multiple times? 
-- yes; took 22ms to run so commenting out
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
-- took 73s for this query to run with massive results...join conditions aren't right 
/*
4	4913
5	315261
6	35212
7	128833
8	54289
9	26325744
12	847189
*/
select sf.study_id, count(distinct a.id) as answer_count
from "ANSWER" a 
join study_info.feature f on f.question_id=a."questionId"
join study_info.intervention_study_feature sf on f.feature_id=sf.feature_id
group by sf.study_id
order by sf.study_id;





-- Castor playaround
select s.*, css.castor_study_id, 'Phase' top_element 
from study_info.study s
join study_info.castor_study_structure css on css.study_id = s.study_id
order by 1 desc; 


