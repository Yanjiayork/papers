with first_try as 
(select ic.hadm_id,
case 
        when ic.icd9_code = '4270' or 
                  ic.icd9_code = '4272' or
                  ic.icd9_code = '42731' or
                  ic.icd9_code = '42732' or
                  ic.icd9_code = '42760' or
                  ic.icd9_code = '42761' or
                  ic.icd9_code = '42769' or
                  ic.icd9_code = '42781' or
                  ic.icd9_code = '42789' or
                  ic.icd9_code = '4279'
then '1'
else '0'
end as af_flag
from DIAGNOSES_ICD ic
group by ic.hadm_id, af_flag),

second_try as 
(select distinct hadm_id, 
MAX(CAST(af_flag AS INTEGER)) OVER (PARTITION BY hadm_id) AS flag_af
from first_try),

epidural_event as 
(select cpt.subject_id, cpt.hadm_id, cpt.cpt_number, cpt.ticket_id_seq, second_try.flag_af,
case 
        when cpt.cpt_number = 62318
        or cpt.cpt_number = 62319
        or cpt.cpt_number = 01996
        then '1'
        else '0'
        end as epidural_flag
from CPTEVENTS cpt
inner join second_try
on cpt.hadm_id = second_try.hadm_id
where cpt.cpt_number between 10004 and 21461
or cpt.cpt_number between 21600 and 69990
or cpt.cpt_number = 01996),

pres as 
(select p.hadm_id, p.startdate, p.enddate, p.drug, p.route,
case 
        when p.drug like '%lol' 
then '1'
else '0'
end as beta_flag
from PRESCRIPTIONS p),

surg as 
(select hadm_id, transfertime, curr_service
from services 
where curr_service = 'CSURG'
or curr_service = 'NSURG'
or curr_service = 'ORTHO'
or curr_service = 'SURG'
or curr_service = 'TSURG'
or curr_service = 'VSURG'
or curr_service = 'DENT'
or curr_service = 'ENT'
or curr_service = 'GU'
or curr_service = 'GYN'
or curr_service = 'PSURG'
or curr_service = 'TRAUM'
order by hadm_id),

beta as
(select pres.hadm_id, pres.beta_flag, 
case 
        when pres.beta_flag = '1' and 
        ROUND(cast(surg.transfertime as date) - cast(pres.startdate as date)) >= 1
        then '1'
        else '0'
        end as Beta_pre,
case
        when beta_flag = '1' and 
        ROUND(cast(surg.transfertime as date) - cast(pres.startdate as date)) >= -1
        and  ROUND(cast(surg.transfertime as date) - cast(pres.startdate as date)) < 1
        then '1'
        when beta_flag = '1' 
        and surg.transfertime < pres.enddate 
        and surg.transfertime > pres.startdate
        then '1'
        else '0'
       end as Beta_post
from pres
inner join surg
on pres.hadm_id = surg.hadm_id
order by hadm_id),

beta_blocker as 
(select distinct hadm_id,
MAX(CAST(beta_flag AS INTEGER)) OVER (PARTITION BY hadm_id) AS flag_beta,
MAX(CAST(beta_pre AS INTEGER)) OVER (PARTITION BY hadm_id) AS pre_beta,
MAX(CAST(beta.beta_post AS INTEGER)) OVER (PARTITION BY hadm_id) AS post_beta
from beta),

hypoten as 
(select cs.hadm_id, cs.charttime, cs.itemid, cs.valuenum, ROW_NUMBER() OVER (partition by cs.hadm_id Order by cs.charttime ) AS rownumber
from chartevents cs
inner join surg su
    on cs.hadm_id = su.hadm_id
    and cs.charttime >= cast(cast(su.transfertime as date) as timestamp without time zone) + interval '30' hour
    and cs.charttime < cast(cast(su.transfertime as date) as timestamp without time zone) + interval '34' hour
where cs.valuenum is not null
and cs.itemid in (220050, 225309, 220179, 51, 6, 6701, 455)
and cs.valuenum != 0 
and cs.error is distinct from 1),
 
lastcheck as (select h.hadm_id, h.charttime, h.itemid, h.valuenum, h.rownumber, MIN(h.rownumber) OVER (PARTITION BY h.hadm_id) as flag_bp
from hypoten h),

hypo_flag as 
(select l.hadm_id, l.valuenum, l.charttime, l.itemid,
case 
when l.valuenum < 100
then '1'
else '0'
end as hypotension_flag
from lastcheck l
where flag_bp = rownumber
order by hadm_id),


rest as 
(select e.hadm_id, e.cpt_number, e.flag_af, 
MAX(CAST(epidural_flag AS INTEGER)) OVER (PARTITION BY e.hadm_id) AS flag_epidural,
be.pre_beta, be.post_beta
from epidural_event e
inner join beta_blocker be
on e.hadm_id = be.hadm_id)

select r.hadm_id, r.cpt_number, r.flag_af, r.flag_epidural, r.pre_beta, r.post_beta, h.hypotension_flag
from rest r
inner join hypo_flag h
on r.hadm_id = h.hadm_id
order by r.hadm_id;
