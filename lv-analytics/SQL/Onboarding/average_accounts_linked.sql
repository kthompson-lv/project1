#### Number of accounts linked, by free and premium users (in Redshift)
#### Requested by Ashley Curran and Scott Yim

# Accounts with 0 transaction are excluded

select avg(count)
from 
(
select user_id, count(*) as count
from legacy.linked_account la
join
(
select linked_account_id as id from
(select linked_account_id, count(*) as count 
from 
legacy.transaction t 
left join legacy.linked_account la on t.linked_account_id = la.id
left join legacy.onboarding_intro oi on oi.user_id = la.user_id 
join 
(select distinct user_id from legacy.user_product 
where purchase_date between '2015-01-01' and '2016-05-31' and purchase_category = 'UPGRADE') up
on up.user_id = oi.user_id
where oi.user_id is not null
and oi.owner = 'CLIENT'
group by 1) x
where count > 0) la1 on la1.id = la.id 
group by 1) y


#AVG for free 4
select avg(count)
from 
(
select user_id, count(*) as count
from legacy.linked_account la
join
(
select linked_account_id as id from
(select linked_account_id, count(*) as count 
from 
legacy.transaction t 
left join legacy.linked_account la on t.linked_account_id = la.id
left join legacy.onboarding_intro oi on oi.user_id = la.user_id 
join legacy.user u on u.id = la.user_id
where oi.user_id is null 
and u.created_on between '2015-01-01' and '2016-05-31' 
group by 1) x
where count > 0) la1 on la1.id = la.id 
group by 1) y
