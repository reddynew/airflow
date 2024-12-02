BEGIN;

TRUNCATE TABLE {{params.target_schema}}.{{params.target_table}}

INSERT INTO {{params.target_schema}}.{{params.target_table}}
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
    , dw_load_date
)
SELECT
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
    , dw_load_date
FROM {{params.stage_schema}}.{{params.stage_table}};

COMMIT;