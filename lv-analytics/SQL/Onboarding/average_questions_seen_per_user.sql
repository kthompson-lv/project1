SELECT count(1) as users
  , round(avg(ccc.ctq::float),0) as Avg
FROM
(SELECT oem.user_id, count(1) AS ctq
FROM legacy.onboarding_entry_history oem LEFT JOIN (
  SELECT oeh.user_id, oeh.entity_key, max(oeh.id) maxid
  FROM legacy.onboarding_entry_history oeh JOIN
        (SELECT DISTINCT up.user_id 
        FROM legacy.user_product up
        JOIN legacy.user u ON u.id = up.user_id
        WHERE up.purchase_date BETWEEN '2015-01-01' AND '2016-05-31' 
            AND up.purchase_category = 'UPGRADE'
            AND u.is_lv_user = 0
            AND u.is_test_user = 0
            AND (u.profile_access <> 'lv_employee' OR u.profile_access IS null)
            AND (up.promo_code NOT IN ('UAT100') OR up.promo_code IS NULL )
            AND (up.promo_code NOT LIKE '%TEST%' OR up.promo_code IS NULL )) uu
        ON uu.user_id=oeh.user_id
  GROUP BY 1,2) oemind
 ON oem.user_id=oemind.user_id AND oem.entity_key=oemind.entity_key AND oem.id=oemind.maxid
WHERE oemind.maxid IS NOT NULL
GROUP BY 1) ccc
