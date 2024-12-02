CREATE TABLE IF NOT EXISTS {{params.target_schema}}.{{params.target_table}}
(
    user_id integer ENCODE az64 distkey,
    user_first_registration_date timestamp without time zone ENCODE raw,
    user_created_date timestamp without time zone ENCODE az64,
    max_event_date_id date ENCODE az64,
    if_used_mobile_first_1 boolean ENCODE raw,
    if_used_mobile_first_7 boolean ENCODE raw,
    if_used_mobile_first_30 boolean ENCODE raw,
    if_used_desktop_first_1 boolean ENCODE raw,
    if_used_desktop_first_7 boolean ENCODE raw,
    if_used_desktop_first_30 boolean ENCODE raw,
    sessions_within_1_to_6_days bigint ENCODE az64,
    visits_session_first_7 bigint ENCODE az64,
    visits_session_first_30 bigint ENCODE az64,
    visits_days_first_7 bigint ENCODE az64,
    count_customise_makeit_project_1 bigint ENCODE az64,
    count_customise_makeit_project_7 bigint ENCODE az64,
    session_after_accountcreated bigint ENCODE az64,
    days_inserted_7 bigint ENCODE az64,
    days_inserted_7_onboarders_home bigint ENCODE az64,
    days_visited_7_onboarders_home bigint ENCODE az64,
    if_clicked_beginner_page integer ENCODE az64,
    if_cut_beginner_page integer ENCODE az64,
    dw_load_date TIMESTAMP WITH TIME ZONE DEFAULT GETDATE() NOT NULL
)
DISTSTYLE KEY
SORTKEY ( user_first_registration_date );

grant select on {{params.target_schema}}.{{params.target_table}} to group data_reader;

DROP TABLE IF EXISTS {{params.stage_schema}}.{{params.stage_table}}{{params.delta_batch}};
CREATE TABLE {{params.stage_schema}}.{{params.stage_table}}{{params.delta_batch}}
(
    user_id integer ENCODE az64,
    user_first_registration_date timestamp without time zone ENCODE az64,
    user_created_date timestamp without time zone ENCODE az64,
    max_event_date_id date ENCODE az64,
    if_used_mobile_first_1 boolean ENCODE raw,
    if_used_mobile_first_7 boolean ENCODE raw,
    if_used_mobile_first_30 boolean ENCODE raw,
    if_used_desktop_first_1 boolean ENCODE raw,
    if_used_desktop_first_7 boolean ENCODE raw,
    if_used_desktop_first_30 boolean ENCODE raw,
    sessions_within_1_to_6_days bigint ENCODE az64,
    visits_session_first_7 bigint ENCODE az64,
    visits_session_first_30 bigint ENCODE az64,
    visits_days_first_7 bigint ENCODE az64,
    count_customise_makeit_project_1 bigint ENCODE az64,
    count_customise_makeit_project_7 bigint ENCODE az64,
    days_inserted_7 bigint ENCODE az64,
    days_inserted_7_onboarders_home bigint ENCODE az64,
    days_visited_7_onboarders_home bigint ENCODE az64,
    if_clicked_beginner_page integer ENCODE az64,
    if_cut_beginner_page integer ENCODE az64,
    dw_load_date TIMESTAMP WITH TIME ZONE DEFAULT GETDATE() NOT NULL
)
DISTSTYLE AUTO;

grant select on {{params.stage_schema}}.{{params.stage_table}}{{params.delta_batch}} to group data_reader;

DROP TABLE IF EXISTS {{params.stage_schema}}.{{params.stage_table}}{{params.union_batch}};
CREATE TABLE {{params.stage_schema}}.{{params.stage_table}}{{params.union_batch}}
(
    user_id integer ENCODE az64,
    user_first_registration_date timestamp without time zone ENCODE az64,
    user_created_date timestamp without time zone ENCODE az64,
    max_event_date_id date ENCODE az64,
    if_used_mobile_first_1 boolean ENCODE raw,
    if_used_mobile_first_7 boolean ENCODE raw,
    if_used_mobile_first_30 boolean ENCODE raw,
    if_used_desktop_first_1 boolean ENCODE raw,
    if_used_desktop_first_7 boolean ENCODE raw,
    if_used_desktop_first_30 boolean ENCODE raw,
    sessions_within_1_to_6_days bigint ENCODE az64,
    visits_session_first_7 bigint ENCODE az64,
    visits_session_first_30 bigint ENCODE az64,
    visits_days_first_7 bigint ENCODE az64,
    count_customise_makeit_project_1 bigint ENCODE az64,
    count_customise_makeit_project_7 bigint ENCODE az64,
    days_inserted_7 bigint ENCODE az64,
    days_inserted_7_onboarders_home bigint ENCODE az64,
    days_visited_7_onboarders_home bigint ENCODE az64,
    if_clicked_beginner_page integer ENCODE az64,
    if_cut_beginner_page integer ENCODE az64,
    dw_load_date TIMESTAMP WITH TIME ZONE DEFAULT GETDATE() NOT NULL
)
DISTSTYLE AUTO;

grant select on {{params.stage_schema}}.{{params.stage_table}}{{params.union_batch}} to group data_reader;


DROP TABLE IF EXISTS {{params.stage_schema}}.{{params.stage_table}}{{params.sessions_batch}};
CREATE TABLE {{params.stage_schema}}.{{params.stage_table}}{{params.sessions_batch}}
(
    user_id integer ENCODE az64,
    max_event_date_id date ENCODE az64,
    session_after_accountcreated bigint ENCODE az64,
    dw_load_date TIMESTAMP WITH TIME ZONE DEFAULT GETDATE() NOT NULL
)
DISTSTYLE AUTO;

grant select on {{params.stage_schema}}.{{params.stage_table}}{{params.sessions_batch}} to group data_reader;

DROP TABLE IF EXISTS {{params.stage_schema}}.{{params.stage_table}};
CREATE TABLE {{params.stage_schema}}.{{params.stage_table}}
(
    user_id integer ENCODE az64,
    user_first_registration_date timestamp without time zone ENCODE az64,
    user_created_date timestamp without time zone ENCODE az64,
    max_event_date_id date ENCODE az64,
    if_used_mobile_first_1 boolean ENCODE raw,
    if_used_mobile_first_7 boolean ENCODE raw,
    if_used_mobile_first_30 boolean ENCODE raw,
    if_used_desktop_first_1 boolean ENCODE raw,
    if_used_desktop_first_7 boolean ENCODE raw,
    if_used_desktop_first_30 boolean ENCODE raw,
    sessions_within_1_to_6_days bigint ENCODE az64,
    visits_session_first_7 bigint ENCODE az64,
    visits_session_first_30 bigint ENCODE az64,
    visits_days_first_7 bigint ENCODE az64,
    count_customise_makeit_project_1 bigint ENCODE az64,
    count_customise_makeit_project_7 bigint ENCODE az64,
    session_after_accountcreated bigint ENCODE az64,
    days_inserted_7 bigint ENCODE az64,
    days_inserted_7_onboarders_home bigint ENCODE az64,
    days_visited_7_onboarders_home bigint ENCODE az64,
    if_clicked_beginner_page integer ENCODE az64,
    if_cut_beginner_page integer ENCODE az64,
    dw_load_date TIMESTAMP WITH TIME ZONE DEFAULT GETDATE() NOT NULL
)
DISTSTYLE AUTO;

grant select on {{params.stage_schema}}.{{params.stage_table}} to group data_reader;
