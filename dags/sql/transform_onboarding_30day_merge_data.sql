BEGIN;

INSERT INTO {{params.stage_schema}}.{{params.stage_table}}
(
    user_id
    , user_first_registration_date
    , user_created_date
    , max_event_date_id
    , if_used_mobile_first_1
    , if_used_mobile_first_7
    , if_used_mobile_first_30
    , if_used_desktop_first_1
    , if_used_desktop_first_7
    , if_used_desktop_first_30
    , sessions_within_1_to_6_days
    , visits_session_first_7
    , visits_session_first_30
    , visits_days_first_7
    , count_customise_makeit_project_1
    , count_customise_makeit_project_7
    , session_after_accountcreated
    , days_inserted_7
    , days_inserted_7_onboarders_home
    , days_visited_7_onboarders_home
    , if_clicked_beginner_page
    , if_cut_beginner_page
)
select distinct
    coalesce(A.user_id,B.user_id) as user_id
    , A.user_first_registration_date as user_first_registration_date
    , A.user_created_date as user_created_date
    , coalesce(B.max_event_date_id,A.max_event_date_id,null) as max_event_date_id
    , coalesce(A.if_used_mobile_first_1,false) as if_used_mobile_first_1
    , coalesce(A.if_used_mobile_first_7,false) as if_used_mobile_first_7
    , coalesce(A.if_used_mobile_first_30,false) as if_used_mobile_first_30
    , coalesce (A.if_used_desktop_first_1,false) as if_used_desktop_first_1
    , coalesce(A.if_used_desktop_first_7,false) as if_used_desktop_first_7
    , coalesce(A.if_used_desktop_first_30,false) as if_used_desktop_first_30
    , coalesce(A.sessions_within_1_to_6_days,0)as sessions_within_1_to_6_days
    , coalesce(A.visits_session_first_7,0) as visits_session_first_7
    , coalesce(A.visits_session_first_30,0) as visits_session_first_30
    , coalesce(A.visits_days_first_7,0)as visits_days_first_7
    , coalesce(A.count_customise_makeit_project_1,0) as count_customise_makeit_project_1
    , coalesce(A.count_customise_makeit_project_7,0) as count_customise_makeit_project_7
    , coalesce(B.session_after_accountcreated,0)as session_after_accountcreated
    , coalesce(A.days_inserted_7,0) as days_inserted_7
    , coalesce(A.days_inserted_7_onboarders_home,0)as days_inserted_7_onboarders_home
    , coalesce(A.days_visited_7_onboarders_home,0)as days_visited_7_onboarders_home
    , coalesce(A.if_clicked_beginner_page,0) as if_clicked_beginner_page
    , coalesce(A.if_cut_beginner_page,0) as if_cut_beginner_page
from {{params.stage_schema}}.{{params.stage_table}}{{params.union_batch}} A
full outer join {{params.stage_schema}}.{{params.stage_table}}{{params.sessions_batch}} B 
on A.user_id=B.user_id
;

COMMIT;