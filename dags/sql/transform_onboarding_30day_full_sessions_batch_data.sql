BEGIN;

INSERT INTO {{params.stage_schema}}.{{params.stage_table}}{{params.sessions_batch}}
(
    user_id
    , max_event_date_id
    , session_after_accountcreated
)
with event_user AS (
SELECT  
event.user_id
,event.event_date_id
, event.session_id
,up.first_registration_date
FROM analytics.firehose_events_enriched event
LEFT JOIN dim.user_profile up ON event.user_id = up.user_id
WHERE
   event_date_id >= '{{params.origination_date}}' and event_date_id<= CURRENT_DATE'{{params.end_date}}'
   and user_first_registration_date>='{{params.origination_date}}'--created_date>= '{{params.origination_date}}' 
   and
  event_name IN ('ApplicationBackgrounded','ApplicationForegrounded','ApplicationLaunched',
                'CanvASEditorAppeared','AdViewed','DestinationPageAppeared','ProjectsSearched',
                 'CanvASTabAppeared','HomeTabAppeared','ImagesSearched')
)
select 
    user_id 
    ,max(event_date_id) max_event_date_id
    , COUNT(DISTINCT session_id) AS session_after_accountcreated
from event_user 
group by 
user_id
;

COMMIT;