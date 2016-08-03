import sys
sys.path.append("../../lv")
import datetime as dt
import lv_query
import json
import string
import pandas as pd
import numpy as np
import sqlalchemy
# import psycopg2

username = user
password = pw
redshift_URL = URL
database = database

engine = sqlalchemy.create_engine(
        "postgresql+psycopg2://user:pw@redshift_URL:5439/database")

def get_purchase_users(startDate, endDate):
    query = """
        select 
            up.user_id
            from user_product up
            join user u on up.user_id = u.id
            join
            (select user_id, max(purchase_date) as last_purchase_date
            from user_product 
            group by 1) x on x.user_id = up.user_id
            left join 
            (SELECT user_id, min(uploaded_on) as uploaded_on
            from periscope.document_merged
            where user_id <> `uploaded_by`
            and type_id in (8000, 3003, 3004, 7000, 4000, 4001, 1004, 7000, 9003)
            group by 1
            ) dm on dm.user_id = up.user_id
            left join partner_plan_participant ppp on ppp.user_id = up.user_id
            where ppp.user_id is null
            and u.is_lv_user = 0
            and u.is_test_user = 0
            and up.promo_code <> 'REPLAN100'
            and u.yodlee_user_id is not null
            and up.`purchase_category` in ('UPGRADE')
            and up.purchase_date > '""" + str(startDate) + """'
            and up.purchase_date < '""" + str(endDate) + """'
        
        """
    table = lv_query.libran(query)
    users = table['user_id'].values
    return(users)

def get_purchase(users, df_list):
    query = """
        select 
            up.user_id,
            last_login_on,
            DATEDIFF(curdate(), last_login_on) as days_since_last_login,
            x.last_purchase_date as last_purchase_date,
            DATEDIFF(curdate(), last_purchase_date) as days_since_purchase,
            up.expired+0 as expired, 
            uploaded_on,
            DATEDIFF(curdate(), uploaded_on) as days_since_uploaded,
            (case when uploaded_on then 1 else 0 end) as plan_delivered, 
            DATEDIFF(uploaded_on, purchase_date) as days_to_delivery
            from user_product up
            join user u on up.user_id = u.id
            join
            (select user_id, max(purchase_date) as last_purchase_date
            from user_product 
            group by 1) x on x.user_id = up.user_id
            left join 
            (SELECT user_id, min(uploaded_on) as uploaded_on
            from periscope.document_merged
            where user_id <> `uploaded_by`
            and type_id in (8000, 3003, 3004, 7000, 4000, 4001, 1004, 7000, 9003)
            group by 1
            ) dm on dm.user_id = up.user_id
            left join partner_plan_participant ppp on ppp.user_id = up.user_id
            where ppp.user_id is null
            and u.is_lv_user = 0
            and u.is_test_user = 0
            and up.promo_code <> 'REPLAN100'
            and u.yodlee_user_id is not null
            and up.`purchase_category` in ('UPGRADE')
            and up.user_id IN (%s) 
            """ % ", ".join(map(str, users))

    
    purchases = lv_query.libran(query)
    purchases['days_to_delivery'] = purchases['days_to_delivery'].apply(lambda x: 0 if x < 0 else x)
    purchases['cohort'] = purchases['last_purchase_date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%m:%s')[:7])
    df_list.append(purchases)
    
    return(df_list)

def get_accounts(users, df_list):
    query = """
        select a.user_id, round(sum(la.curr_act_balance),0) as assets_balance, 
        b.liabilities as liabilities_balance, count(1) as account_count, 
        sum(case when a.container_type = 'bank' then 1 else 0 end) as bank_accounts,
        sum(case when a.container_type = 'stocks' then 1 else 0 end) as stocks_accounts,
        b.count_credits, b.count_loans as l
        from onboarding_account a
        join   
        linked_account la on la.id = a.linked_account_id
        join
        (select oa.user_id, round(sum(la.curr_act_balance),0) as liabilities,
        sum(case when la.container_type = 'credits' then 1 else 0 end) as count_credits,
        sum(case when la.container_type = 'loans' then 1 else 0 end) as count_loans
        from onboarding_account oa
        join linked_account la on la.id = oa.linked_account_id
        where oa.container_type in ('credits','loans')
        and oa.active = 'S' and oa.user_id IN (%s)
        group by 1) b
        on a.user_id = b.user_id
        where a.container_type in ('bank','cash','stocks')
        and a.active = 'S' and a.user_id IN (%s)
        group by 1;
        """ % (", ".join(map(str,users)), ", ".join(map(str,users)))

    linked_accounts = lv_query.libran(query)
    df_list.append(linked_accounts)
    
    return(df_list)

def get_onboarding(users, df_list):
    query = """
        select * from 
        (
        select oei.user_id, ifnull(sum(obei.annual_income),'') as annual_income, ifnull(op.ownership,'') as home_ownership, 
        TIMESTAMPDIFF(YEAR, oi.birthday, curdate()) as age,
        (case when oi.relationship_status = 'SINGLE' then 1 else 0 end) as single ,
        obi.feels_about_managing_finances as confidence,
        (case when obi.have_children = 'YES' then 1 else 0 end) as 'have_children',
        oei.employment_type as employment_type
        from onboarding_intro oi  
        join onboarding_basics_info obi on obi.user_id = oi.user_id
        join onboarding_budget_employment_income obei on oi.user_id = obei.user_id
        join onboarding_property_info op on oi.user_id = op.user_id
        join onboarding_employment_info oei on oei.user_id = oi.user_id
        where oei.owner = 'CLIENT' and oei.active = 'S'
        and oei.user_id IN (%s)
        group by 1 ) x
        left join
        (select oei.user_id as user_id2, sum(annual_income) as partner_income, oei.employment_type as partner_employment_type
        from onboarding_intro oi
        left join onboarding_employment_info oei on oei.user_id = oi.user_id
        left join onboarding_budget_employment_income obei on oei.user_id = obei.user_id
        and oei.id = obei.employment_info_id
        where relationship_status <> 'SINGLE'
        and oei.owner = 'partner'
        and obei.owner='partner'
        group by 1) y
        on x.user_id = y.user_id2
    """ % ", ".join(map(str,users))

    onboarding = lv_query.libran(query)
    onboarding.drop('user_id2', inplace=True, axis=1)

    def full_part_time(x):
        if x == 'FULL_TIME' or x =='PART_TIME':
            return 1
        else:
            return 0
    
    onboarding['FT_PT_employment']= onboarding['employment_type'].apply(lambda x: full_part_time(x))
    onboarding['partner_FT_PT_employment']= onboarding['partner_employment_type'].apply(lambda x: full_part_time(x))
    df_list.append(onboarding)
    
    return(df_list)

def get_call_data(users, df_list):
    query = """
        SELECT
            cs.user_id, ss_complete.start_time as 'SS_call_completed', ss_scheduled.start_time as 'SS_call_scheduled',
            action_complete.start_time as 'Action_call_completed', action_scheduled.start_time as 'Action_call_scheduled'
            FROM coach_slot cs
            left join
            (select user_id, cs.start_time
            from coach_slot cs
            join user u on u.id = cs.user_id
            where cs.call_status_id = 103 and cs.call_type_id = 200
            and u.is_lv_user = 0
            and u.is_test_user = 0
            and (u.profile_access <> 'lv_employee' or u.profile_access is null)                
            and cs.call_status_id = 103) ss_complete   
            on ss_complete.user_id = cs.user_id   
            
            left join 
            (select user_id, max(cs.start_time) as start_time
            from coach_slot cs
            join user u on u.id = cs.user_id
            where cs.call_type_id = 200
            and (cs.call_status_id = 103 or cs.call_status_id = 102)
            and u.is_lv_user = 0
            and u.is_test_user = 0
            and (u.profile_access <> 'lv_employee' or u.profile_access is null)               
            group by 1) ss_scheduled   
            on ss_scheduled.user_id = cs.user_id  
            
            left join 
            (select user_id, cs.start_time
            from coach_slot cs
            join user u on u.id = cs.user_id
            where cs.call_status_id = 103 and cs.call_type_id = 201
            and u.is_lv_user = 0
            and u.is_test_user = 0
            and (u.profile_access <> 'lv_employee' or u.profile_access is null)                
            and cs.call_status_id = 103) action_complete   
            on action_complete.user_id = cs.user_id   

            left join 
            (select user_id, cs.start_time
            from coach_slot cs
            join user u on u.id = cs.user_id
            where cs.call_type_id = 201
            and (cs.call_status_id = 103 or cs.call_status_id = 102)
            and u.is_lv_user = 0
            and u.is_test_user = 0
            and (u.profile_access <> 'lv_employee' or u.profile_access is null)              
            group by 1) action_scheduled
            on action_scheduled.user_id = cs.user_id    
            where cs.user_id IN (%s)
            group by 1
        """ % ", ".join(map(str,users))

    calls = lv_query.libran(query)
    df_list.append(calls)

    return(df_list)

def get_plan(users, df_list):
    query = """
    SELECT user_id, uploaded_on, (case when uploaded_on then 1 else 0 end) as plan_delivered
    from periscope.document_merged
    where user_id <> `uploaded_by`
    and type_id in (8000, 3003, 3004, 7000, 4000, 4001, 1004, 7000, 9003)
    and user_id IN (%s)
    group by 1
    """ % ", ".join(map(str,users))
        
    plan_delivered = lv_query.libran(query)
    df_list.append(plan_delivered)

    return(df_list)

def get_challenges(users, df_list=None):
    query = """
    select c.client_id as user_id, count(*) as challenges_active, sum(case when status = 'ACHIEVED' then 1 else 0 end) as challlenges_achieved
    from periscope.challenge_merged c
    where c.status not in ('PENDING')
    and c.is_active = 1
    and curdate() between c.start_date and c.end_date
    and c.client_id IN (%s) 
    group by 1
    """ % ", ".join(map(str,users))
        
    challenges = lv_query.libran(query)
    df_list.append(challenges)

    return(df_list)

def get_planner(users, df_list=None):
    query = """
        select a.planner_id, a.user_id, b.num_clients
        from user_planner a
        join 
        (select planner_id, count(user_id) as num_clients
        from user_planner upl
        join user u
        on u.id = upl.user_id
        where u.id IN (%s) 
        group by planner_id
        ) b
        on a.planner_id = b.planner_id
        """ % ", ".join(map(str,users))

    planner = lv_query.libran(query)
    df_list.append(planner)

    return(df_list)

def merge_df(df_list):
    #merged = df_list[0].merge(df_list[1], how= 'left', on='user_id')
    df_final = reduce(lambda left,right: pd.merge(left,right,on='user_id', how='left'), df_list)
    return(df_final)

def clean_merged_df(df):
    df = df.replace(['None', np.nan], '', regex=True)
    df = df.replace('', np.nan, regex=True)

    def call_action(x):
        if pd.isnull(x):
            return 0
        else:
            return 1

    def confidence_map(text):
        if text == 'confident':
            return 3
        elif text == 'unsure':
            return 2
        elif text == 'overwhelmed':
            return 1
        else:
            return 0    

    df[['SS_call_completed', 'SS_call_scheduled', 'Action_call_completed', 'Action_call_scheduled']]= df[['SS_call_completed', 'SS_call_scheduled', 'Action_call_completed', 'Action_call_scheduled']].applymap(call_action)
    df['own_home'] = df['home_ownership'].apply(lambda x: 1 if x == 'OWN' else x if pd.isnull(x) else 0)
    df['annual_income'] = df['annual_income'].apply(lambda x: float(x))
    df['household_income'] = df['annual_income'] + df['partner_income']
    df['confidence'] = df['confidence'].apply(lambda x: confidence_map(x))
    df['income'] = df['annual_income']
    df['income'][df['single'] ==0] = df['household_income']

    return(df)

if __name__ == '__main__':
    all_dfs = []

    startDate = dt.date(2015,1,1)
    endDate = dt.date(2016,7,1)
    users = get_purchase_users(startDate, endDate)

    fl = [get_purchase, get_accounts, get_onboarding, get_call_data, get_plan, get_challenges, get_planner]
    [f(users, all_dfs) for f in fl]

    print "success with rows " + str(len(all_dfs))

    merged = merge_df(all_dfs)
    print "successful merge"
    
    final = clean_merged_df(merged)
    final['retrieved_on'] = dt.datetime.now().date()
    print "successful cleaning merged df"
    
    #final.to_csv("client_data.csv")
    final.to_sql('client_data', engine, if_exists='append', index=False)
    print "successful transfer to redshift"





