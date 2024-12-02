BEGIN;

INSERT INTO {{params.stage_schema}}.{{params.stage_table}}{{params.delta_batch}}
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
with existing_users as(
select user_id from {{params.stage_schema}}.{{params.stage_table}}
where user_first_registration_date >= DATE_ADD(DAY, -45, GETDATE())  --{{params.start_date}} 
),

event_user AS (
SELECT distinct event.user_id,event_name,screenname,
	session_id,event_date_id,event_timestamp,
	event_attributes,pageid,imageid,projectid,clientplatformname, 
	up.first_registration_date , up.created_date
FROM analytics.firehose_events_enriched event
    LEFT JOIN dim.user_profile up ON event.user_id = up.user_id
    WHERE 
        event_date_id >=DATE_ADD(DAY, -45, GETDATE())  and event_date_id<=CURRENT_DATE    --{{params.start_date}}
        AND (created_date between DATE_ADD(DAY, -45, GETDATE()) and CURRENT_DATE or first_registration_date between DATE_ADD(DAY, -45, GETDATE()) and CURRENT_DATE) -- {{params.start_date}}
		AND 
        event_name NOT IN 
            ( 'ApiABTestTriggered', 'ApiCanvASSaved', 'ApiContentImageUploaded', 'ApiContentSearched',
            'ApiMachineConnected', 'ApiMachineRegistered', 'ApiUserAccountCreated', 'CanvASTabAppeared',
            'ClientABTestTriggered', 'ClientBridgeError', 'ClientCutBridgeError', 'MachineActivationError', 
            'MachineConnectionError', 'MachineFirmwareUpdateError', 'NotificationCreated',
            'OOBMachineActivationError', 'OOBMachineConnectionError', 'OOBTestCutError' 
            )
	),
	project_image_insert AS (

	SELECT eu.user_id ,event_name,
	(coalesce(imageid,projectid) || date(eu.event_date_id)::varchar || eu.user_id::varchar) AS unionall,
	(coalesce(dp.source_project_id)::varchar || date(event_date_id)::varchar || eu.user_id::varchar) AS unionall_source_project,
      	(coalesce(eu.projectid)::varchar || date(event_date_id)::varchar || eu.user_id::varchar) AS unionall_project
	FROM event_user eu left join dim.project dp on eu.projectid = dp.project_id
	where event_date_id <= first_registration_date+6
	and event_name IN ('ProjectMakeItClicked','ProjectCustomizeClicked','ImageInserted','ClientMatItemCutPassEnd')
	group by eu.user_id,event_name,imageid,projectid,event_date_id,source_project_id
	),

    new_users as(
	SELECT distinct  COALESCE(eu.user_id, au.user_id) AS user_id,event_name,screenname,
	session_id,coalesce(eu.event_date_id,au.max_event_date_id) AS event_date_id,event_timestamp,
	event_attributes,pageid,imageid,projectid,clientplatformname,
	  COALESCE(eu.first_registration_date, au.user_first_registration_date) AS first_registration_date ,
	  COALESCE(eu.created_date, au.user_created_date) AS created_date from event_user eu
	full outer join existing_users au on eu.user_id=au.user_id

	)
SELECT 
    user_id,first_registration_date as user_first_registration_date,created_date as user_created_date,max(event_date_id) max_event_date_id,
    bool_or(
    CASE 
        WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) = 0 
        AND clientplatformname in ('IOS','ANDROID')
        THEN true else false END) AS if_used_mobile_first_1,
    bool_or(CASE 
        WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) BETWEEN 1 AND 6
        AND clientplatformname in ('IOS','ANDROID')
        THEN true else false END) AS if_used_mobile_first_7,
    bool_or(CASE 
        WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) BETWEEN 7 AND 29 
        AND clientplatformname in ('IOS','ANDROID')
        THEN true else false END) AS if_used_mobile_first_30,
    bool_or(CASE
        WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) = 0
        AND clientplatformname in ('WINDOWS','MACOS')
        THEN true else false END) AS if_used_desktop_first_1,
    bool_or(CASE
        WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) BETWEEN 1 AND 6
        AND clientplatformname in ('WINDOWS','MACOS')
        THEN true else false END) AS if_used_desktop_first_7,
    bool_or(CASE
        WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) BETWEEN 7 AND 29
        AND clientplatformname in ('WINDOWS','MACOS')
        THEN true else false END) AS if_used_desktop_first_30,
    COUNT(DISTINCT CASE
        WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) BETWEEN 1 AND 6
        THEN session_id
        END) AS sessions_within_1_to_6_days,
    COUNT(DISTINCT CASE 
        WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) <= 6 AND DATEDIFF(days, first_registration_date, date(event_date_id)) >=0 THEN session_id 
        END) AS visits_session_first_7,
    COUNT(DISTINCT CASE 
        WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) <= 29 AND DATEDIFF(days, first_registration_date, date(event_date_id)) >=0 THEN session_id 
        END) AS visits_session_first_30,
    COUNT(DISTINCT CASE 
        WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) <= 6
        AND date(first_registration_date)<= date(event_date_id) THEN event_date_id::date
        END) AS visits_days_first_7,
    COUNT(DISTINCT CASE 
            WHEN event_name IN ('ProjectMakeItClicked', 'ProjectCustomizeClicked') AND screenname <> 'CANVAS_EDITOR' AND DATEDIFF(days, first_registration_date, date(event_date_id)) = 0
            THEN projectid
        END) AS count_customise_makeit_project_1,
        COUNT(DISTINCT CASE 
            WHEN event_name IN ('ProjectMakeItClicked', 'ProjectCustomizeClicked') AND screenname <> 'CANVAS_EDITOR' AND DATEDIFF(days, first_registration_date, date(event_date_id)) <= 6 AND DATEDIFF(days, first_registration_date, date(event_date_id)) >=0 
            THEN projectid
        END) AS count_customise_makeit_project_7,
    COUNT( DISTINCT 
	CASE WHEN event_name IN ('ImageInserted','FontInserted')
	    AND screenname NOT ILIKE '%canvas%' 
	    AND screenname NOT ILIKE '%MY_STUFF%'
	    AND screenname NOT ILIKE '%IMAGE_DETAILS%'
	    AND screenname NOT ILIKE '%Search%'
	    AND DATEDIFF(days, first_registration_date, date(event_date_id)) <= 6
	    AND DATEDIFF(days, first_registration_date, date(event_date_id)) >= 0
	OR 
	    event_name = 'TileboxTileClicked' 
	    AND pageid in ('4','156','1','3')
	    AND DATEDIFF(day,first_registration_date, event_date_id) <= 6 
	    AND DATEDIFF(day, first_registration_date, date(event_date_id)) >= 0
	    AND (event_attributes.tileObjectId::varchar || date(event_date_id)::varchar || user_id::varchar) 
	    IN (SELECT DISTINCT unionall FROM project_image_insert WHERE event_name IN ('ProjectMakeItClicked','ProjectCustomizeClicked','ImageInserted'))
	OR 
	    event_name = 'InspirationTileClicked'  
	    AND DATEDIFF(day,first_registration_date, event_date_id) <= 6
	    AND DATEDIFF(day, first_registration_date, date(event_date_id)) >= 0
	    AND (event_attributes.tileObjectId::varchar || date(event_date_id)::varchar || user_id::varchar) 
	    IN (SELECT DISTINCT unionall FROM project_image_insert WHERE event_name IN ('ProjectMakeItClicked','ProjectCustomizeClicked','ImageInserted'))
	THEN event_date_id
	    END
	) AS days_inserted_7,

	COUNT( DISTINCT 
	CASE WHEN 
	event_name = 'TileboxTileClicked' 
	AND pageid in ('4','156')
	AND DATEDIFF(day,first_registration_date, event_date_id) <= 6 
	AND DATEDIFF(day, first_registration_date, date(event_date_id)) >= 0
	AND (event_attributes.tileObjectId::varchar || date(event_date_id)::varchar || user_id::varchar) 
	IN (SELECT DISTINCT unionall FROM project_image_insert WHERE event_name IN ('ProjectMakeItClicked','ProjectCustomizeClicked','ImageInserted'))

	THEN event_date_id
		END
	) AS days_inserted_7_onboarders_home,
    COUNT( DISTINCT
        CASE WHEN
            event_name = 'DestinationPageAppeared'
        AND pageid in ('4','156')
        AND DATEDIFF(day,first_registration_date, event_date_id) <= 6
        AND DATEDIFF(day, first_registration_date, date(event_date_id)) >= 0
    THEN event_date_id
        END
    ) AS days_visited_7_onboarders_home,
MAX(CASE WHEN  		
  event_name = 'TileboxTileClicked' 
  AND pageid IN ('4','156') 
  AND DATEDIFF(day,first_registration_date, event_date_id) <= 6 
  AND DATEDIFF(day,first_registration_date, event_date_id) >= 0
  THEN 1 ELSE 0 End
) as if_clicked_beginner_page,
MAX(CASE WHEN  		
  event_name = 'TileboxTileClicked' 
  AND pageid IN ('4','156')
  AND DATEDIFF(day,first_registration_date, event_date_id) <= 6 
  AND DATEDIFF(day,first_registration_date, event_date_id) >= 0
  AND (
    (
      (event_attributes.tileObjectId::varchar || date(event_date_id)::varchar || user_id::varchar) 
		IN (SELECT DISTINCT unionall_source_project FROM project_image_insert where event_name = 'ClientMatItemCutPassEnd')
    )
    OR (
      (event_attributes.tileObjectId::varchar || date(event_date_id)::varchar || user_id::varchar) 
		IN (SELECT DISTINCT unionall_project FROM project_image_insert where event_name = 'ClientMatItemCutPassEnd')
    )
  )
  THEN 1 ELSE 0 End
) as if_cut_beginner_page

    FROM  
    new_users
    GROUP BY 
    user_id,first_registration_date,created_date
    ;

    COMMIT;
-- with existing_users as(
-- select user_id,user_first_registration_date,user_created_date,max_event_date_id 
-- from {{params.target_schema}}.{{params.target_table}}
-- -- public.onboarding_jv_first_30_behaviors_final_tbl
-- where user_first_registration_date >= '{{params.start_date}}' -- '2024-09-16' 
-- or user_first_registration_date is null
-- ),
-- event_user AS (
-- SELECT distinct event.user_id,event_name,screenname,
-- 	session_id,event_date_id,event_timestamp,
-- 	event_attributes,pageid,imageid,projectid,clientplatformname, 
-- 	up.first_registration_date , up.created_date
-- FROM analytics.firehose_events_enriched event
--     LEFT JOIN dim.user_profile up ON event.user_id = up.user_id
--     WHERE 
--         event_date_id between '{{params.start_date}}' and '{{params.end_date}}' -- >= '2024-09-16' and event_date_id<='2024-11-05'
--         AND (created_date between '{{params.start_date}}' and '{{params.end_date}}' -- between '2024-09-16' and '2024-11-05' 
--         or 
--         	first_registration_date between '{{params.start_date}}' and '{{params.end_date}}' --between '2024-09-16' and '2024-11-05'
--         	)
-- 		AND 
--         event_name NOT IN 
--             ( 'ApiABTestTriggered', 'ApiCanvASSaved', 'ApiContentImageUploaded', 'ApiContentSearched',
--             'ApiMachineConnected', 'ApiMachineRegistered', 'ApiUserAccountCreated', 'CanvASTabAppeared',
--             'ClientABTestTriggered', 'ClientBridgeError', 'ClientCutBridgeError', 'MachineActivationError', 
--             'MachineConnectionError', 'MachineFirmwareUpdateError', 'NotificationCreated',
--             'OOBMachineActivationError', 'OOBMachineConnectionError', 'OOBTestCutError' 
--             )
-- 	),
-- 	project_image_insert AS (

-- 	SELECT eu.user_id ,event_name,
-- 	(coalesce(imageid,projectid) || date(eu.event_date_id)::varchar || eu.user_id::varchar) AS unionall,
-- 	(coalesce(dp.source_project_id)::varchar || date(event_date_id)::varchar || eu.user_id::varchar) AS unionall_source_project,
--       	(coalesce(eu.projectid)::varchar || date(event_date_id)::varchar || eu.user_id::varchar) AS unionall_project
-- 	FROM event_user eu left join dim.project dp on eu.projectid = dp.project_id
-- 	where event_date_id <= first_registration_date+6
-- 	and event_name IN ('ProjectMakeItClicked','ProjectCustomizeClicked','ImageInserted','ClientMatItemCutPassEnd')
-- 	group by eu.user_id,event_name,imageid,projectid,event_date_id,source_project_id
-- 	),
--     new_users as(
-- 	SELECT distinct  COALESCE(eu.user_id, au.user_id) AS user_id,event_name,screenname,
-- 	session_id,coalesce(eu.event_date_id,au.max_event_date_id) AS event_date_id,event_timestamp,
-- 	event_attributes,pageid,imageid,projectid,clientplatformname,
-- 	  COALESCE(eu.first_registration_date, au.user_first_registration_date) AS first_registration_date ,
-- 	  COALESCE(eu.created_date, au.user_created_date) AS created_date from event_user eu
-- 	full outer join existing_users au on eu.user_id=au.user_id

-- 	)
-- SELECT 
--     user_id,first_registration_date as user_first_registration_date,created_date as user_created_date,max(event_date_id) max_event_date_id,
--     bool_or(
--     CASE 
--         WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) = 0 
--         AND clientplatformname in ('IOS','ANDROID')
--         THEN true else false END) AS if_used_mobile_first_1,
--     bool_or(CASE 
--         WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) BETWEEN 1 AND 6
--         AND clientplatformname in ('IOS','ANDROID')
--         THEN true else false END) AS if_used_mobile_first_7,
--     bool_or(CASE 
--         WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) BETWEEN 7 AND 29 
--         AND clientplatformname in ('IOS','ANDROID')
--         THEN true else false END) AS if_used_mobile_first_30,
--     bool_or(CASE
--         WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) = 0
--         AND clientplatformname in ('WINDOWS','MACOS')
--         THEN true else false END) AS if_used_desktop_first_1,
--     bool_or(CASE
--         WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) BETWEEN 1 AND 6
--         AND clientplatformname in ('WINDOWS','MACOS')
--         THEN true else false END) AS if_used_desktop_first_7,
--     bool_or(CASE
--         WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) BETWEEN 7 AND 29
--         AND clientplatformname in ('WINDOWS','MACOS')
--         THEN true else false END) AS if_used_desktop_first_30,
--     COUNT(DISTINCT CASE
--         WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) BETWEEN 1 AND 6
--         THEN session_id
--         END) AS sessions_within_1_to_6_days,
--     COUNT(DISTINCT CASE 
--         WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) <= 6 AND DATEDIFF(days, first_registration_date, date(event_date_id)) >=0 THEN session_id 
--         END) AS visits_session_first_7,
--     COUNT(DISTINCT CASE 
--         WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) <= 29 AND DATEDIFF(days, first_registration_date, date(event_date_id)) >=0 THEN session_id 
--         END) AS visits_session_first_30,
--     COUNT(DISTINCT CASE 
--         WHEN DATEDIFF(days, first_registration_date, date(event_date_id)) <= 6
--         AND date(first_registration_date)<= date(event_date_id) THEN event_date_id::date
--         END) AS visits_days_first_7,
--     COUNT(DISTINCT CASE 
--             WHEN event_name IN ('ProjectMakeItClicked', 'ProjectCustomizeClicked') AND screenname <> 'CANVAS_EDITOR' AND DATEDIFF(days, first_registration_date, date(event_date_id)) = 0
--             THEN projectid
--         END) AS count_customise_makeit_project_1,
--         COUNT(DISTINCT CASE 
--             WHEN event_name IN ('ProjectMakeItClicked', 'ProjectCustomizeClicked') AND screenname <> 'CANVAS_EDITOR' AND DATEDIFF(days, first_registration_date, date(event_date_id)) <= 6 AND DATEDIFF(days, first_registration_date, date(event_date_id)) >=0 
--             THEN projectid
--         END) AS count_customise_makeit_project_7,
--     COUNT( DISTINCT 
-- 	CASE WHEN event_name IN ('ImageInserted','FontInserted')
-- 	    AND screenname NOT ILIKE '%canvas%' 
-- 	    AND screenname NOT ILIKE '%MY_STUFF%'
-- 	    AND screenname NOT ILIKE '%IMAGE_DETAILS%'
-- 	    AND screenname NOT ILIKE '%Search%'
-- 	    AND DATEDIFF(days, first_registration_date, date(event_date_id)) <= 6
-- 	    AND DATEDIFF(days, first_registration_date, date(event_date_id)) >= 0
-- 	OR 
-- 	    event_name = 'TileboxTileClicked' 
-- 	    AND pageid in ('4','156','1','3')
-- 	    AND DATEDIFF(day,first_registration_date, event_date_id) <= 6 
-- 	    AND DATEDIFF(day, first_registration_date, date(event_date_id)) >= 0
-- 	    AND (event_attributes.tileObjectId::varchar || date(event_date_id)::varchar || user_id::varchar) 
-- 	    IN (SELECT DISTINCT unionall FROM project_image_insert WHERE event_name IN ('ProjectMakeItClicked','ProjectCustomizeClicked','ImageInserted'))
-- 	OR 
-- 	    event_name = 'InspirationTileClicked'  
-- 	    AND DATEDIFF(day,first_registration_date, event_date_id) <= 6
-- 	    AND DATEDIFF(day, first_registration_date, date(event_date_id)) >= 0
-- 	    AND (event_attributes.tileObjectId::varchar || date(event_date_id)::varchar || user_id::varchar) 
-- 	    IN (SELECT DISTINCT unionall FROM project_image_insert WHERE event_name IN ('ProjectMakeItClicked','ProjectCustomizeClicked','ImageInserted'))
-- 	THEN event_date_id
-- 	    END
-- 	) AS days_inserted_7,

-- 	COUNT( DISTINCT 
-- 	CASE WHEN 
-- 	event_name = 'TileboxTileClicked' 
-- 	AND pageid in ('4','156')
-- 	AND DATEDIFF(day,first_registration_date, event_date_id) <= 6 
-- 	AND DATEDIFF(day, first_registration_date, date(event_date_id)) >= 0
-- 	AND (event_attributes.tileObjectId::varchar || date(event_date_id)::varchar || user_id::varchar) 
-- 	IN (SELECT DISTINCT unionall FROM project_image_insert WHERE event_name IN ('ProjectMakeItClicked','ProjectCustomizeClicked','ImageInserted'))

-- 	THEN event_date_id
-- 		END
-- 	) AS days_inserted_7_onboarders_home,
--     COUNT( DISTINCT
--         CASE WHEN
--             event_name = 'DestinationPageAppeared'
--         AND pageid in ('4','156')
--         AND DATEDIFF(day,first_registration_date, event_date_id) <= 6
--         AND DATEDIFF(day, first_registration_date, date(event_date_id)) >= 0
--     THEN event_date_id
--         END
--     ) AS days_visited_7_onboarders_home,
-- MAX(CASE WHEN  		
--   event_name = 'TileboxTileClicked' 
--   AND pageid IN ('4','156') 
--   AND DATEDIFF(day,first_registration_date, event_date_id) <= 6 
--   AND DATEDIFF(day,first_registration_date, event_date_id) >= 0
--   THEN 1 ELSE 0 End
-- ) as if_clicked_beginner_page,
-- MAX(CASE WHEN  		
--   event_name = 'TileboxTileClicked' 
--   AND pageid IN ('4','156')
--   AND DATEDIFF(day,first_registration_date, event_date_id) <= 6 
--   AND DATEDIFF(day,first_registration_date, event_date_id) >= 0
--   AND (
--     (
--       (event_attributes.tileObjectId::varchar || date(event_date_id)::varchar || user_id::varchar) 
-- 		IN (SELECT DISTINCT unionall_source_project FROM project_image_insert where event_name = 'ClientMatItemCutPassEnd')
--     )
--     OR (
--       (event_attributes.tileObjectId::varchar || date(event_date_id)::varchar || user_id::varchar) 
-- 		IN (SELECT DISTINCT unionall_project FROM project_image_insert where event_name = 'ClientMatItemCutPassEnd')
--     )
--   )
--   THEN 1 ELSE 0 End
-- ) as if_cut_beginner_page

--     FROM  
--     new_users
--     GROUP BY 
--     user_id,first_registration_date,created_date
--     ;

--     COMMIT;