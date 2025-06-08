BEGIN
    CREATE TEMP TABLE cdc_table AS
    SELECT 
        *,
        CASE 
            WHEN mod_rec.id IS NULL THEN 'Insert' 
            WHEN new_mod_rec.id_new IS NULL THEN 'Delete' 
            WHEN mod_rec.id = new_mod_rec.id_new AND mod_rec.specialization = new_mod_rec.specialization_new THEN 'Update' 
            ELSE 'Undefined'
        END AS Operation_flag
    FROM 
        (
            -- Records modified in Source (existing in target_table but not in stg_student_engagement)
            SELECT id, firstName, lastName, specialization, country, Total_DE_Score, total_ml_score, level, attemptsCodingTaskSQL, attemptsCodingTaskPython, createdOn, lastRequest, updatedOn, email_address
            FROM `mh-project-testing.hive12.target_table`
            EXCEPT DISTINCT
            SELECT id, firstName, lastName, specialization, country, Total_DE_Score, total_ml_score, level, attemptsCodingTaskSQL, attemptsCodingTaskPython, createdOn, lastRequest, updatedOn, email_address
            FROM `mh-project-testing.hive12.stg_student_engagement`
        ) AS mod_rec
    FULL JOIN 
        (
            -- New/Modified Records in Source (existing in stg_student_engagement but not in target_table)
            SELECT id AS id_new, firstName AS firstName_new, lastName AS lastName_new, specialization AS specialization_new, country AS country_new, Total_DE_Score AS Total_DE_Score_new, total_ml_score AS total_ml_score_new, level AS level_new, attemptsCodingTaskSQL AS attemptsCodingTaskSQL_new, attemptsCodingTaskPython AS attemptsCodingTaskPython_new, createdOn AS createdOn_new, lastRequest AS lastRequest_new, updatedOn AS updatedOn_new, email_address AS email_address_new
            FROM `mh-project-testing.hive12.stg_student_engagement`
            EXCEPT DISTINCT
            SELECT id, firstName, lastName, specialization, country, Total_DE_Score, total_ml_score, level, attemptsCodingTaskSQL, attemptsCodingTaskPython, createdOn, lastRequest, updatedOn, email_address
            FROM `mh-project-testing.hive12.target_table`
        ) AS new_mod_rec
    ON new_mod_rec.id_new = mod_rec.id AND new_mod_rec.specialization_new = mod_rec.specialization;

    -- Insert records  
    INSERT INTO `mh-project-testing.hive12.target_table` 
    SELECT id_new AS id, firstName_new AS firstName, lastName_new AS lastName, specialization_new AS specialization, country_new AS country, Total_DE_Score_new AS Total_DE_Score, total_ml_score_new AS total_ml_score, level_new AS level, attemptsCodingTaskSQL_new AS attemptsCodingTaskSQL, attemptsCodingTaskPython_new AS attemptsCodingTaskPython, createdOn_new AS createdOn, lastRequest_new AS lastRequest, updatedOn_new AS updatedOn, email_address_new AS email_address, CURRENT_TIMESTAMP() AS created_dt, CURRENT_TIMESTAMP() AS last_updated_dt, 'Stored Proc' AS last_updated_by
    FROM cdc_table WHERE Operation_flag='Insert';

    -- Delete records
    DELETE FROM `mh-project-testing.hive12.target_table` cdc
    WHERE EXISTS (
        SELECT 1 
        FROM cdc_table tmp 
        WHERE tmp.Operation_flag = 'Delete' 
        AND cdc.id = tmp.id 
        AND cdc.specialization = tmp.specialization
    );

    -- Update records
    UPDATE `mh-project-testing.hive12.target_table` cdc 
    SET 
        firstName = firstName_new,
        lastName = lastName_new, 
        specialization = specialization_new, 
        country = country_new, 
        Total_DE_Score = Total_DE_Score_new, 
        total_ml_score = total_ml_score_new, 
        level = level_new, 
        attemptsCodingTaskSQL = attemptsCodingTaskSQL_new, 
        attemptsCodingTaskPython = attemptsCodingTaskPython_new, 
        createdOn = createdOn_new, 
        lastRequest = lastRequest_new, 
        updatedOn = updatedOn_new, 
        email_address = email_address_new, 
        last_updated_dt = CURRENT_TIMESTAMP(),
        last_updated_by = 'Stored Proc'
    FROM cdc_table tmp
    WHERE tmp.Operation_flag = 'Update' AND cdc.id = tmp.id AND cdc.specialization = tmp.specialization;

    -- Update run_control_table
    INSERT INTO `mh-project-testing.hive12.run_control_tbl`
    SELECT 'target_table' AS tbl_nm, 'stg_student_engagement' AS src_tbl_nm, SUM(IF(Operation_flag='Insert',1,0)) AS rec_inserted, SUM(IF(Operation_flag='Delete',1,0)) AS rec_deleted, SUM(IF(Operation_flag='Update',1,0)) AS rec_updated, CURRENT_TIMESTAMP() AS run_dt
    FROM cdc_table;

    -- Clean up staging table for next load
    DELETE FROM `mh-project-testing.hive12.stg_student_engagement` where true ;

    -- Drop the temporary table
    DROP TABLE cdc_table;
END
