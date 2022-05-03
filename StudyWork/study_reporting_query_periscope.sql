--justin.thomson@viome.com
-- one study reporting query to rule them all


with notifications as (
	select iscft.study_id, count(distinct cfr.user_id) as Users_Contacted
	from chat_flow_request cfr
	join ai_info.chat_flow_template cft on cft.id=cfr.chat_flow_template_id
	join study_info.intervention_study_chat_flow_template iscft on iscft.chat_flow_template_id=cft.id
	group by iscft.study_id
), 
stoolkitsamples as (
	select sp.study_id, count(distinct k."kitId") as StoolCount
	from study_info.study_participant sp
	join study_info.participant p on p.participant_id=sp.participant_id  
	join "KIT" k on k."userId"=p.user_id
	join kit_type kt on kt.id = k.kit_type_id and k.kit_type_id=1 -- 1=GI
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
qanda as (
-- get question and answer data 
-- study_info.response stores answers from outside viome online
-- public.ANSWER stores answers from viome online
-- output needs to be combination of the two 
-- feature can be used in multiple studies, but response can only be in one
	select sf.study_id, count(distinct sf.feature_id) as Questions, count(a.*) as Answers 
	from study_info.intervention_study_feature sf
	left join study_info.response a on a.feature_id=sf.feature_id and a.study_id=sf.study_id
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
