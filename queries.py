create_trusted_main_list = """
CREATE OR REPLACE TABLE trusted.main_list AS
SELECT 
    Id as id
    ,"First Name" || ' ' || "Last Name" as fighter_name
FROM raw.main_list;
"""

create_trusted_fighters_list = """
    CREATE OR REPLACE TABLE trusted.fighters_list AS
    SELECT DISTINCT
        match_id
        ,FL.id
        ,ML1.fighter_name as fighter_name
        ,CAST(opponent_id AS INT64) AS opponent_id
        ,COALESCE(ML2.fighter_name, opponent) AS opponent_name
        ,result
        ,method
        ,CASE WHEN UPPER(method) LIKE '%PTS%'                                                                        THEN 'POINTS' 
              WHEN UPPER(method) LIKE '%POINTS%'                                                                     THEN 'POINTS' 
              WHEN UPPER(method) LIKE '%ADV%'                                                                        THEN 'POINTS' 
              WHEN UPPER(method) LIKE '%PEN%'                                                                        THEN 'POINTS'
              WHEN UPPER(method) LIKE '%DECISION%'                                                                   THEN 'DECISION'
              WHEN UPPER(method) LIKE '%DQ%'                                                                         THEN 'DESQUALIFICATION'
              WHEN UPPER(method) LIKE '%INJUR%'                                                                      THEN 'INJURY'
              ELSE 'SUBMISSION'
        END AS win_type
        ,CASE WHEN UPPER(method) LIKE '%GUILLOTINE%'                                                                 THEN 'Guillotine' 
              WHEN UPPER(method) LIKE '%RNC%'                                                                        THEN 'RNC' 
              WHEN UPPER(method) LIKE '%HEEL HOOK%' AND UPPER(method) LIKE '%INSIDE%'                                THEN 'Inside heel hook'
              WHEN UPPER(method) LIKE '%HEEL HOOK%' AND UPPER(method) LIKE '%OUTSIDE%'                               THEN 'Outside heel hook'
              WHEN UPPER(method) LIKE '%HEEL HOOK%'                                                                  THEN 'Heel hook' 
              WHEN UPPER(method) LIKE '%ARMBAR%' OR UPPER(method) LIKE '%ARMLOCK%'                                   THEN 'Armbar'
              WHEN UPPER(method) LIKE '%KIMURA%'                                                                     THEN 'Kimura' 
              WHEN UPPER(method) LIKE '%TRIANGLE%'                                                                   THEN 'Triangle' 
              WHEN UPPER(method) LIKE '%TOE HOLD%'                                                                   THEN 'Toe hold' 
              WHEN UPPER(method) LIKE '%DARCE%'                                                                      THEN 'D\'\'arce choke'
              WHEN UPPER(method) LIKE '%KATAGATAME%'                                                                 THEN 'Katagatame'
              WHEN UPPER(method) LIKE '%NORTH%' AND UPPER(method) LIKE '%SOUTH%'                                     THEN 'North south choke' 
              WHEN UPPER(method) LIKE '%ANKLE%' OR UPPER(method) LIKE '%BOTINHA%' OR UPPER(method) LIKE '%FOOTLOCK%' THEN 'Footlock' 
              WHEN UPPER(method) LIKE '%LEG LOCK%' OR UPPER(method) LIKE '%LEGLOCK%'                                 THEN 'Leg lock'
              WHEN UPPER(method) LIKE '%PTS%'                                                                        THEN NULL 
              WHEN UPPER(method) LIKE '%POINTS%'                                                                     THEN NULL 
              WHEN UPPER(method) LIKE '%ADV%'                                                                        THEN NULL 
              WHEN UPPER(method) LIKE '%PEN%'                                                                        THEN NULL
              WHEN UPPER(method) LIKE '%DECISION%'                                                                   THEN NULL
              WHEN UPPER(method) LIKE '%DQ%'                                                                         THEN NULL
              WHEN UPPER(method) LIKE '%INJURY%'                                                                     THEN NULL
              ELSE method
        END AS submission,
        CASE WHEN UPPER(method) LIKE '%PTS%' AND UPPER(method) LIKE '%,%'                                            THEN CAST(SPLIT_PART(SUBSTR(method, 6, INSTR(method, ',') - 6), 'x', 1) AS INT64)
            WHEN UPPER(method) LIKE '%PTS%'                                                                          THEN CAST(SPLIT_PART(TRIM(SUBSTR(method, 6)), 'x', 1) AS INT64)
            ELSE NULL
        END AS winner_points,
        CASE WHEN UPPER(method) LIKE '%PTS%' AND UPPER(method) LIKE '%,%'                                            THEN CAST(SPLIT_PART(SUBSTR(method, 6, INSTR(method, ',') - 6), 'x', 2) AS INT64)
            WHEN UPPER(method) LIKE '%PTS%'                                                                          THEN CAST(SPLIT_PART(TRIM(SUBSTR(method, 6)), 'x', 2) AS INT64)
            ELSE NULL
        END AS loser_points
        ,CASE WHEN UPPER(method) LIKE '%PTS%' AND UPPER(method) LIKE '%PEN%'                                         THEN 'PEN'
              WHEN UPPER(method) LIKE '%PTS%' AND UPPER(method) LIKE '%ADV%'                                         THEN 'ADV'
              ELSE NULL
        END AS adv_pen
        ,competition
        ,CASE WHEN weight LIKE '%60KG' THEN 'F'
              ELSE 'M'
        END AS sex
        ,REPLACE(weight, 'O', '+') AS weight_class
        ,stage
        ,year
    FROM raw.fighters_list FL
    LEFT JOIN trusted.main_list ML1 ON ML1.id = FL.id
    LEFT JOIN trusted.main_list ML2 ON ML2.id = FL.opponent_id 
    WHERE competition = 'ADCC';
"""

create_refined_fight_record = """
CREATE OR REPLACE TABLE refined.fight_record AS
WITH FIGHTS AS (
    SELECT 
        match_id,
        id as winner_id,
        fighter_name as winner_name,
        opponent_id as loser_id,
        opponent_name as loser_name,
        win_type,
        submission,
        winner_points,
        loser_points,
        adv_pen,
        weight_class,
        sex,
        stage,
        year
    FROM trusted.fighters_list
    WHERE result = 'W'
      AND match_id in (SELECT match_id
                       FROM trusted.fighters_list
                       GROUP BY match_id
                       HAVING COUNT(*) > 1)
    UNION ALL 
    SELECT 
        match_id,
        id as winner_id,
        fighter_name as winner_name,
        opponent_id as loser_id,
        opponent_name as loser_name,
        win_type,
        submission,
        winner_points,
        loser_points,
        adv_pen,
        weight_class,
        sex,
        stage,
        year
    FROM trusted.fighters_list
    WHERE result = 'W'
      AND match_id in (SELECT match_id
                       FROM trusted.fighters_list
                       GROUP BY match_id
                       HAVING COUNT(*) = 1)
    UNION ALL
    SELECT 
        match_id,
        opponent_id as winner_id,
        opponent_name as winner_name,
        id as loser_id,
        fighter_name as loser_name,
        win_type,
        submission,
        winner_points,
        loser_points,
        adv_pen,
        weight_class,
        sex,
        stage,
        year
    FROM trusted.fighters_list
    WHERE result = 'L'
      AND match_id in (SELECT match_id
                       FROM trusted.fighters_list
                       GROUP BY match_id
                       HAVING COUNT(*) = 1)
)
SELECT 
    IFNULL(match_id, -1) AS match_id,
    IFNULL(winner_id, -1) AS winner_id,
    IFNULL(winner_name, 'N/A') AS winner_name,
    IFNULL(loser_id, -1) AS loser_id,
    IFNULL(loser_name, 'N/A') AS loser_name,
    IFNULL(win_type, 'N/A') AS win_type,
    IFNULL(submission, 'N/A') AS submission,
    IFNULL(winner_points, -1) AS winner_points,
    IFNULL(loser_points, -1) AS loser_points,
    IFNULL(adv_pen, 'N/A') AS adv_pen,
    IFNULL(weight_class, 'N/A') AS weight_class,
    IFNULL(sex, 'N/A') AS sex,
    IFNULL(stage, 'N/A') AS stage,
    IFNULL(year, -1) AS year
FROM FIGHTS;
"""