SELECT 
DISTINCT(legacy.onboarding_goal.goal_type),
date_part(year,legacy.priority_goal.created_dt),
COUNT(*)
FROM legacy.onboarding_goal 
JOIN legacy.onboarding_basics_info 
ON legacy.onboarding_goal.user_id = legacy.onboarding_basics_info.user_id
JOIN legacy.priority_goal
ON legacy.onboarding_goal.user_id = legacy.priority_goal.user_id
WHERE legacy.onboarding_basics_info.owner = 'CLIENT' 
AND legacy.onboarding_goal.goal_type != 'RETIREMENT' AND legacy.onboarding_goal.goal_type != 'EMERGENCY_SAVINGS' AND legacy.onboarding_goal.goal_type != 'CC_DEBT'
AND legacy.onboarding_goal.goal_type_group != 'BASIC_SECURITY' 
GROUP BY legacy.onboarding_goal.goal_type,
date_part(year,legacy.priority_goal.created_dt)
ORDER BY date_part(year,legacy.priority_goal.created_dt) DESC
