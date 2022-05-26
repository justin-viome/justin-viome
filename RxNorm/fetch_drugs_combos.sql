-- justin.thomson@viome.com

-- get RxNorm data for CN, Engineering teams
-- need to map drugs->ATC codes->contraindications/more for recommendation engine


-- generate full list of drugname+ATCs including brand names and generics 
-- https://viomeinc.atlassian.net/browse/VRX-21
with d as (
	select distinct rxcui, STR from RXNCONSO where sab = 'RXNORM'
),
a as (
	select rxcui, CODE, STR from RXNCONSO where sab = 'ATC'
),
generics as (
	select distinct a.STR drug, a.ATC
	from RXNREL r 
	join d on d.rxcui = r.rxcui1 
	join a on a.rxcui = r.rxcui2 and RELA = 'has_tradename'
), brands as (
	select distinct d.STR drug, a.ATC
	from RXNREL r 
	join d on d.rxcui = r.rxcui1 
	join a on a.rxcui = r.rxcui2 and RELA = 'has_tradename'
),
alldrugs as (
	select * from generics
	union 
	select * from brands 
)
select replace(replace(drug, ',', '-'), '"', '''') as drug, ATC 
from alldrugs
order by drug asc;


-- generate full list of generic+brand names for engineering 
-- replace commas in data with semicolons
with d as (
	select distinct rxcui, STR from RXNCONSO where sab = 'RXNORM'
),
a as (
	select rxcui, CODE, STR from RXNCONSO where sab = 'ATC'
),
generics as (
	select distinct a.STR drug
	from RXNREL r 
	join d on d.rxcui = r.rxcui1 
	join a on a.rxcui = r.rxcui2 and RELA = 'has_tradename'
), brands as (
	select distinct d.STR drug
	from RXNREL r 
	join d on d.rxcui = r.rxcui1 
	join a on a.rxcui = r.rxcui2 and RELA = 'has_tradename'
),
alldrugs as (
	select * from generics
	union 
	select * from brands 
)
select replace(replace(drug, ',', '-'), '"', '''') as drug 
from alldrugs
order by drug asc;


-- full list of brand/generic/ATCs
-- shared with Grant/CN team
with d as (
select distinct rxcui, STR from RXNCONSO where sab = 'RXNORM'
),
a as (
select rxcui, CODE, STR from RXNCONSO where sab = 'ATC'
),
allout as (
	select distinct a.STR generic, d.STR brand, a.CODE ATC
	from RXNREL r 
	join d on d.rxcui = r.rxcui1 
	join a on a.rxcui = r.rxcui2 and RELA = 'has_tradename'
)
select a.brand, a.generic, a.ATC  
from allout a
order by 1,2,3;

-- full list w groups, combos only
-- shared with Grant/CN team 
with d as (
select distinct rxcui, STR from RXNCONSO where sab = 'RXNORM'
),
a as (
select rxcui, CODE, STR from RXNCONSO where sab = 'ATC'
),
allout as (
	select distinct d.rxcui, a.STR generic, d.STR brand, a.CODE ATC
	from RXNREL r 
	join d on d.rxcui = r.rxcui1 
	join a on a.rxcui = r.rxcui2 and RELA = 'has_tradename'
),
comboz as (
	select RXCUI  from allout a
	group by RXCUI
	having count(*)>1
)
select a.brand, a.generic, a.ATC 
from allout a
join comboz c on c.rxcui=a.rxcui
-- where a.brand='Excedrin'
order by 1,2,3;

-- concatenated by brand only 
with d as (
select distinct rxcui, STR from RXNCONSO where sab = 'RXNORM'
),
a as (
select rxcui, CODE, STR from RXNCONSO where sab = 'ATC'
)

SELECT d.STR brand, GROUP_CONCAT(a.STR order by a.STR) AS generic_list, GROUP_CONCAT(a.code order by a.STR) as atc_list
from RXNREL r 
join d on d.rxcui = r.rxcui1 
join a on a.rxcui = r.rxcui2 and RELA = 'has_tradename'
where d.STR='Excedrin'
GROUP  BY d.STR;

-- concatenated by ATC
with d as (
select distinct rxcui, STR from RXNCONSO where sab = 'RXNORM'
),
a as (
select rxcui, CODE, STR from RXNCONSO where sab = 'ATC'
)

SELECT d.STR brand, a.STR as generic, GROUP_CONCAT(a.code order by a.STR) as atc_list
from RXNREL r 
join d on d.rxcui = r.rxcui1 
join a on a.rxcui = r.rxcui2 and RELA = 'has_tradename'
where d.STR='Excedrin'
GROUP  BY d.STR, a.STR;


-- query to get combination drugs 
with d as (
select distinct rxcui, STR, tty from RXNCONSO where sab = 'RXNORM'
)

-- select * from d where d.STR='Excedrin' -- gets rxcui of 217020
select *  
from RXNREL r 
join d on d.rxcui = r.rxcui1 
where d.STR='Excedrin'

-- rxnorm stores relationships in both direcitons
with combos as (
select distinct rxcui, STR
from RXNCONSO 
where sab = 'RXNORM'  and tty in ('GPCK', 'BPCK')
and str like '%xcedrin%'
),
a as (
select rxcui, CODE, STR from RXNCONSO where sab = 'ATC'
)
select * from RXNREL r
join combos c on c.rxcui=r.RXCUI2 or c.rxcui=r.RXCUI1 ;

with combos as (
select distinct rxcui, STR
from RXNCONSO 
where sab = 'RXNORM'  and tty in ('GPCK', 'BPCK')
and str like '%xcedrin%'
),
a as (
select rxcui, CODE, STR from RXNCONSO where sab = 'ATC'
)
select * from RXNREL r
join combos c on c.rxcui=r.RXCUI2 or c.rxcui=r.RXCUI1 ;

-- need to get from cui=217020 (tradename reference of excedrin) to cui=2047428 (tty=gpck reference of excedrin)

select * from RXNREL WHERE rxcui1=2047427
-- -> dose form of -> 746839


select * from RXNREL WHERE rxcui1=746839 and RELA != 'has_dose_form'
-- -> dose form of -> 746839




select * from RXNCONSO r  where r.RXAUI =1912878





