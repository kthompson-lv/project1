SELECT 
 DATE_FORMAT(ops.submission_dt, '%Y-%m') AS DATE
,count(DISTINCT user_id) AS submitted_accounts
FROM onboarding_profile_status ops
JOIN user u ON ops.user_id = u.id
WHERE 
u.is_lv_user = 0
AND u.is_test_user = 0
AND u.yodlee_user_id IS NOT NULL
AND [u.created_on=daterange]
AND ops.last_saved_point IN ('#/snapshot','#/submission')
AND ops.submission_dt IS NOT NULL
GROUP BY
 1
