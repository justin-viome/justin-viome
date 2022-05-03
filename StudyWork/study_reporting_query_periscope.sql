--justin.thomson@viome.com
-- one study reporting query to rule them all

-- questions
-- for chat_flow_requests, add a "study_id" column and populate it based on the name column?
-- will need this so query updates not needed to get chat flow data for future studies

with notifications as (
-- add studyid to info from chat flow requests for easier reporting 
	select case when cft.id=2 then 1
		when cft.id=11 then 12 
		when cft.id=12 then 4
		when cft.id=13 then 6
		when cft.id=14 then 5
		when cft.id=16 then 7
		when cft.id=18 then 8
		when cft.id=19 then 9
		end as study_id, count(distinct cfr.user_id) as Users_Contacted
	from chat_flow_request cfr
	join ai_info.chat_flow_template cft on cft.id=cfr.chat_flow_template_id
	group by 1
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
qanda as (
-- get question and answer data 
	select sf.study_id, count(distinct sf.feature_id) as Questions, count(a.*) as Answers 
	from study_info.intervention_study_feature sf
	left join study_info.response a on a.feature_id=sf.feature_id
	group by sf.study_id
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
	nt.Users_Contacted,
	s.Participants,
	samp.Stool_Samples, samp.Blood_Samples, samp.Saliva_Samples,
	q.Questions, q.Answers
from baseinfo s
left join notifications nt on nt.study_id=s.study_id
left join studsamples samp on samp.study_id=s.study_id
left join qanda q on q.study_id=s.study_id
order by s.study_id asc;
