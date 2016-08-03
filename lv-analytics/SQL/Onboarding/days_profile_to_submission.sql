Objective: Calculate number of days between users starting their profile and submitting them
Requested by: Maddy

select sum(case when days = 0 then 1 else 0 end) as same_day_submission,
sum(case when days > 30 and days < 90 then 1 else 0 end) as submission_between_30_90_days,
sum(case when days > 90 then 1 else 0 end) as submission_after_90_day
from
(
select oeh.user_id, profile_started, submission_dt, datediff(submission_dt, profile_started) as days
from
(select user_id, min(created_dt) as profile_started
from legacy.onboarding_entry_history
group by 1) oeh
join (select id as user_id, u.* from legacy.user u) u on u.user_id = oeh.user_id
join legacy.user_product up on oeh.user_id = up.user_id
left join legacy.partner_plan_participant ppp on ppp.user_id = up.user_id
left join legacy.onboarding_profile_status ops on ops.user_id = oeh.user_id
where purchase_date between '2015-01-01' and '2016-05-31' and purchase_category = 'UPGRADE'
and is_lv_user = 0
and is_test_user = 0
and (profile_access <> 'lv_employee' or profile_access is null)
and (up.promo_code not in ('UAT100') or up.promo_code is null)
and (up.promo_code not like '%TEST%' or up.promo_code is null)
and ppp.user_id is null) x;
