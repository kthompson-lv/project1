#### How many clients linked any accounts during onboarding? Breakdown by D2C and LVW
#### Requested by Ashley Curran and Scott Yim

#D2C 
select oem, count(*)
from 
(
select * 
from
(select oem.user_id as oem, oem.entity_key, created_dt as account_linked_ts, oeh.user_id as oeh, 
oeh.profile_started, ops.submission_dt, ops.last_saved_dt
from onboarding_entry_metadata oem
join 
(select distinct up.user_id from user_product up
join user u on u.id = up.user_id
left join partner_plan_participant ppp on ppp.user_id = up.user_id
where purchase_date between '2015-01-01' and '2016-05-31' and purchase_category = 'UPGRADE'
and is_lv_user = 0
and is_test_user = 0
and (profile_access <> 'lv_employee' or profile_access is null)
and (up.promo_code not in ('UAT100') or up.promo_code is null )
and (up.promo_code not like '%TEST%' or up.promo_code is null )
and ppp.user_id is null) up
on up.user_id = oem.user_id
left join
(select user_id, min(created_dt) as profile_started
from onboarding_entry_history
group by 1) oeh
on oeh.user_id = oem.user_id
left join onboarding_profile_status ops on ops.user_id = oeh.user_id
where entity_key = 'acct_institution_name' and (submission_dt is not null or last_saved_point IN ("#/snapshot", "#/submission"))
group by 1, 2, 3
) x
where account_linked_ts > profile_started and (account_linked_ts < submission_dt or account_linked_ts < last_saved_dt)
) y
group by 1;

#2910

## ALL
select distinct oem.user_id
from onboarding_entry_metadata oem
join 
(select distinct up.user_id from user_product up
join user u on u.id = up.user_id
left join partner_plan_participant ppp on ppp.user_id = up.user_id
where purchase_date between '2015-01-01' and '2016-05-31' and purchase_category = 'UPGRADE'
and is_lv_user = 0
and is_test_user = 0
and (profile_access <> 'lv_employee' or profile_access is null)
and (up.promo_code not in ('UAT100') or up.promo_code is null )
and (up.promo_code not like '%TEST%' or up.promo_code is null )
and ppp.user_id is null) up
on up.user_id = oem.user_id
#3644

#LVW
select oem, count(*)
from 
(
select * 
from
(select oem.user_id as oem, oem.entity_key, created_dt as account_linked_ts, oeh.user_id as oeh, 
oeh.profile_started, ops.submission_dt, ops.last_saved_dt
from onboarding_entry_metadata oem
join 
(select user_id from partner_plan_participant ppp
join user u where u.id = ppp.user_id and u.created_on between '2015-01-01' and '2016-05-31') ppp
on ppp.user_id = oem.user_id
left join
(select user_id, min(created_dt) as profile_started
from onboarding_entry_history
group by 1) oeh
on oeh.user_id = oem.user_id
left join onboarding_profile_status ops on ops.user_id = oeh.user_id
where entity_key = 'acct_institution_name' and (submission_dt is not null or last_saved_point IN ("#/snapshot", "#/submission"))
group by 1, 2, 3
) x
where account_linked_ts > profile_started and (account_linked_ts < submission_dt or account_linked_ts < last_saved_dt)
) y
group by 1;
#1434


## ALL
select distinct oem.user_id
from onboarding_entry_metadata oem
join (select user_id from partner_plan_participant ppp
	join user u where u.id = ppp.user_id and u.created_on between '2015-01-01' and '2016-05-31') ppp
on ppp.user_id = oem.user_id
#3685
