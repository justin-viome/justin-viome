select s.study_id, s.name, s.description, count(sp.*) as Participant_Count
from study_info.study s
left join study_info.study_participant sp on s.study_id=sp.study_id
group by s.study_id
order by 1 asc;

-- get count of users contacted for each study
select cft.name, cft.description, count(distinct cfr.user_id) as DistinctUsersContacted
from chat_flow_request cfr
join ai_info.chat_flow_template cft on cft.id=cfr.chat_flow_template_id
group by cfr.chat_flow_template_id, cft.name, cft.description
order by 1,2,3;

-- get number of questions asked 

-- get number of questions answered 

-- are any questions answered multiple times? 
-- yes; took 22ms to run so commenting out
--select a."userId", a.questionnaire_id, count(*) as answer_count 
--from "ANSWER" a
--group by a."userId", a.test_id
--order by 3 desc;

-- questionnaire_id is always null 
select count(*) from "ANSWER" where questionnaire_id is not null 