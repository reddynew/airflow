BEGIN;

INSERT INTO {{params.stage_schema}}.{{params.stage_table}}{{params.union_batch}}
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
    , days_inserted_7
    , days_inserted_7_onboarders_home
    , days_visited_7_onboarders_home
    , if_clicked_beginner_page
    , if_cut_beginner_page
)
select 
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
    , days_inserted_7
    , days_inserted_7_onboarders_home
    , days_visited_7_onboarders_home
    , if_clicked_beginner_page
    , if_cut_beginner_page
from {{params.target_schema}}.{{params.target_table}} --onboarding_jv_first_30_behavior_metrics(in analytics schema)
where user_first_registration_date < DATE_ADD(DAY, -45, GETDATE())--'{{params.end_date}}'--'2024-09-16'
 union
select 
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
    , days_inserted_7
    , days_inserted_7_onboarders_home
    , days_visited_7_onboarders_home
    , if_clicked_beginner_page
    , if_cut_beginner_page
from  {{params.stage_schema}}.{{params.stage_table}}{{params.delta_batch}} 
;

COMMIT;