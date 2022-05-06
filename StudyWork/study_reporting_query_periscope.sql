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
-- responsible for 8s/8.6s of run
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
baseinfo as (
	select s.study_id, s.name, s.description, count(distinct sp.participant_id) as Participants
	from study_info.study s
	left join notifications nt on nt.study_id=s.study_id
	left join study_info.study_participant sp on s.study_id=sp.study_id
	left join studsamples samp on samp.study_id=s.study_id
	group by s.study_id
) 

select s.study_id, s.name, s.description, 
	coalesce(nt.Users_Contacted,0) as Users_Contacted,
	s.Participants,
	(coalesce(samp.Stool_Samples, 0) + coalesce(sampstool.StoolCount, 0)) as Stool_Samples, 
	coalesce(samp.Blood_Samples, 0) as Blood_Samples,
	coalesce(samp.Saliva_Samples,0) as Saliva_Samples,
	(coalesce(q.Questions_sif, 0) + coalesce(q2.Questions_a, 0)) as Questions, 
	(coalesce(q.Answers_sif, 0) + coalesce(q2.Answers_a, 0)) as Answers
from baseinfo s
left join notifications nt on nt.study_id=s.study_id
left join studsamples samp on samp.study_id=s.study_id
left join stoolkitsamples sampstool on sampstool.study_id=s.study_id
left join qanda_sif q on q.study_id=s.study_id
left join qanda_answers q2 on q2.study_id=s.study_id
order by s.study_id asc;
