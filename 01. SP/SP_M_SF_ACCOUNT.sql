CREATE OR REPLACE PROCEDURE HDL.MRT.SP_M_SF_ACCOUNT(p_etl_type varchar(30), p_base_dt VARCHAR(10), p_call_in_info VARCHAR(2000))
RETURNS INT
LANGUAGE SQL
COMMENT = '마트영역 적재'
EXECUTE AS CALLER
AS
$$
/*==================================================================================================================
  01. 작업설명 : DM 적재 (Salesforce)
  02. 소스 : HDL.MRT.M_HM_ACCOUNT
           HDL.INT.I_SF_ACCOUNT
           HDL.INT.I_SF_USER
           HDL.INT.I_SF_COUNTRY
           HDL.INT.I_HM_M_CODE_DETAIL
  03. 타겟 : MRT.M_SF_ACCOUNT
  04. Parameter : p_etl_type, p_base_dt, p_call_in_info
  05. 수작업실행 예제: CALL HDL.MRT.SP_M_SF_ACCOUNT('SCHEDULE','2023-11-03','');
   ------------------------------------------------------------------------------------------------------------------
 (버전)   (수정일자)     (수정자)       (내용)
  0.1     2023-09     MZC 김서연    최초 작성
  0.1     2023-10     HL  양은주    최초 작성, 로직 수정, DELETE 추가 등
================================================================================================================== */
DECLARE
    -- sp parameters
    lv_etl_type         VARCHAR := TRIM(p_etl_type) ;
    lv_base_date        VARCHAR := TRIM(p_base_dt) ;
	lv_call_in_info     VARCHAR := TRIM(p_call_in_info) ;
	
	-- custom variables
    lv_proc_nm          VARCHAR := 'HDL.MRT.SP_M_SF_ACCOUNT' ;
    lv_job_type         VARCHAR := 'SFDC' ;

    -- log variables
	lv_log_msg          VARCHAR DEFAULT '';
    lv_log_msg_flow     VARCHAR DEFAULT '';
    lv_log_status       VARCHAR DEFAULT 'SUCCESS';
	lv_proc_str_dt      VARCHAR DEFAULT '';
	lv_proc_end_dt      VARCHAR DEFAULT '';
    lv_step_str_dt      VARCHAR DEFAULT '';
    lv_step_end_dt      VARCHAR DEFAULT '';
	lv_row_cnt          INT     DEFAULT 0 ;
    lv_step_no          VARCHAR DEFAULT '000';
	
	EXCEPTION_1 EXCEPTION (-20001, '[Wrong Parameter] \'p_base_dt\' is Invalid DATE value.');  -- (SQLCODE, SQLERRM)
    EXCEPTION_2 EXCEPTION (-20002, '[Wrong Parameter] \'p_etl_type\' is Empty or Invalid.') ;  -- (SQLCODE, SQLERRM)
BEGIN
    -- ****** Set Local Variables ******
    SELECT TO_VARCHAR(CONVERT_TIMEZONE('Asia/Seoul', CURRENT_TIMESTAMP()),'YYYY-MM-DD HH24:MI:SS')
	INTO lv_proc_str_dt ;

    -- lv_etl_type
    IF (lv_etl_type IS NULL OR lv_etl_type = '' OR UPPER(lv_etl_type) NOT IN ('SCHEDULE','MANUAL','FULL')) THEN
	    RAISE EXCEPTION_2;
    ELSEIF (lv_etl_type = 'MANUAL' AND (lv_base_date IS NULL OR lv_base_date = '')) THEN
        RAISE EXCEPTION_1;
    ELSE lv_etl_type := UPPER(lv_etl_type);
	END IF;
    
    -- lv_base_date (default = TODAY)
    IF (lv_base_date IS NULL OR lv_base_date = '') THEN
       SELECT TO_VARCHAR(CONVERT_TIMEZONE('Asia/Seoul', CURRENT_TIMESTAMP()),'YYYY-MM-DD')
       INTO lv_base_date;
    END IF;
    IF (TRY_TO_DATE(lv_base_date) IS NULL) THEN 
	   RAISE EXCEPTION_1; -- Wrong Date format
	END IF;
    
    -- Write Log
    lv_log_msg := '000. START';
    CALL HDL.INT._SP_WRITE_LOG(:lv_call_in_info, :lv_job_type, 'PROCEDURE', :lv_proc_nm, :lv_proc_str_dt, :lv_proc_str_dt, 'SUCCESS', :lv_step_no, :lv_etl_type, :lv_base_date, :lv_row_cnt, :lv_log_msg);
    lv_log_msg_flow := lv_log_msg;

    -- ****** Run Tasks ******
    BEGIN TRANSACTION ;
    --------------------------------------------------------------
    -- [01. 단계별 작업]
    ----------------------------------------
    -- STEP-01
    ----------------------------------------
    lv_step_no := '001';
    SELECT TO_VARCHAR(CONVERT_TIMEZONE('Asia/Seoul', CURRENT_TIMESTAMP()),'YYYY-MM-DD HH24:MI:SS') INTO lv_step_str_dt ;

    -- LOGIC => 로직 추가
    DELETE FROM HDL.MRT.M_SF_ACCOUNT;
	
    -- 로그메시지 변경
    lv_log_msg := '001. DELETE(ALL)';
    lv_row_cnt := SQLROWCOUNT ;
    SELECT TO_VARCHAR(CONVERT_TIMEZONE('Asia/Seoul', CURRENT_TIMESTAMP()),'YYYY-MM-DD HH24:MI:SS') INTO lv_step_end_dt ;
    CALL HDL.INT._SP_WRITE_LOG(:lv_call_in_info, :lv_job_type, 'PROCEDURE', :lv_proc_nm, :lv_step_str_dt, :lv_step_end_dt, 'SUCCESS', :lv_step_no, :lv_etl_type, :lv_base_date, :lv_row_cnt, :lv_log_msg);
    lv_log_msg_flow := lv_log_msg_flow||'>'||lv_log_msg;

    ----------------------------------------
    -- STEP-02
    ----------------------------------------
    lv_step_no := '002';
    SELECT TO_VARCHAR(CONVERT_TIMEZONE('Asia/Seoul', CURRENT_TIMESTAMP()),'YYYY-MM-DD HH24:MI:SS') INTO lv_step_str_dt ;

    -- LOGIC => 로직 추가
    INSERT INTO HDL.MRT.M_SF_ACCOUNT
       SELECT T2.ID                                AS AccountId
             ,T1.CUSTR_NM                          AS AccountName            /* 거래처명 */
            -- ,'TEST_거래처명123'||T1.CUSTR_CD  AS Name_CustrNm                /* 거래처명 */   		
    	     ,CASE WHEN SUBSTR(T1.CUSTR_CD,1,2) IN ('HB','B') THEN 'Customer'  
    	           WHEN SUBSTR(T1.CUSTR_CD,1,2) IN ('S','HS') THEN 'Partner'  
    		   	ELSE Null END  AS TYPE  
    	     ,CASE WHEN SUBSTR(T1.CUSTR_CD,1,2) IN ('HB','B') THEN '0125j000000l5zeAAA'  
    	           WHEN SUBSTR(T1.CUSTR_CD,1,2) IN ('S','HS') THEN '0125j000001NgEWAA0'  
    		   	ELSE Null END  AS RECORDTYPEID  
             ,T1.CUR_CD                            AS CurrencyIsoCode               /* 통화코드 */
             ,NVL(T3.ID,'0055j000006BJSgAAO') 	   AS OwnerId                       /* 추가필드10(사용자ID) 예:0055j000006C2InAAK */				            
             ,T1.CUSTR_FULLNM                      AS Short_name__c                 /* 거래처명 */
           --  ,'TEST_거래처full명456'||T1.CUSTR_CD     AS Short_name__c               /* 거래처명 */
             ,T1.TRS_CND                           AS Incoterms__c                  /* 운송조건 */
             ,T4.ID                                AS Country__c                    /* 예:a0v5j000002wyA8AAI */
             ,T1.REGN_NM	                       AS Region__c                     /* 지역명  */
             ,T1.CUSTR_CD                          AS HOMS_No__c                    /* 거래처코드 */
            -- ,'TEST_'||T1.CUSTR_CD                 AS HOMS_No__c                  /* 거래처코드 */
             ,T1.BUYER_TYP                         AS Acc_Type__c  	                /* 바이어타입 */
    		 /* ASIS: 신프로님 HOMS화면에서 UTC->KST로 변경 후 SFDC에 upsert하고 있음 */
     		 /* TOBE: 20230907엄p-HOMS DB에 저장된 UTC 를 그대로 SFDC에 upsert 하기로 함. sfdc화면에서 kST로 설정 후 조회하기로 함 */
             ,T1.ERP_CUSTR_NO                      AS ERP_No__c 	                /* ERP거래처번호 */             
             ,T1.CORPOR_CD                         AS Division_Code__c   	        /* 법인코드 */  
             ,T1.CORPOR_NM                         AS Division__c                   /* 법인명   */
    		 ,T1.REG_USER_NM                       AS CREATOR__C
             ,T1.CHG_USER_NM                       AS MODIFIER__C
             ,T1.REG_DTM                           AS CREATED_DATE__C
             ,T1.CHG_DTM                           AS MODIFIED_DATE__C
             ,T1.REG_IP                            AS CREATORIP__C
             ,T1.CHG_IP                            AS MODIFIERIP__C
             ,T1.REG_DTM	                       AS Registration_date__c          /* 등록일자(10자리)->등록일시(timestamp)로 SFDC 타입변경 */
    	     ,CURRENT_TIMESTAMP()::timestamp_ntz                  AS ETL_REG_DTM                   /* ETL등록일시 */
    	     ,CURRENT_TIMESTAMP()::timestamp_ntz                  AS ETL_CHG_DTM                   /* ETL변경일시 */
    	   --  ,T1.ADD_FILD_10                       AS EMPLOYEENUMBER__v             /* 사용자 */
    	   --  ,T1.NATN_CD                           AS NATN_CD__v                    /* 국가코드  */
           --  ,T1.REGN_CD	                       AS Region__c__v                  /* 지역코드  */
    FROM HDL.MRT.M_HM_ACCOUNT T1
         LEFT OUTER join
    	 HDL.INT.I_SF_ACCOUNT T2
      ON T1.CUSTR_CD = T2.HOMS_NO__C
     AND T2.HOMS_NO__C IS NOT NULL
         LEFT OUTER JOIN
    	 HDL.INT.I_SF_USER T3 
      ON T1.ADD_FILD_10 = T3.EMPLOYEENUMBER /* 예:P14382 */
     AND T3.ISACTIVE = 'TRUE'
     AND T3.PROFILEID <> '00e5j000003R9W2AAK'  /* CHATTER 인 경우 제외 */
         LEFT OUTER JOIN
    	 HDL.INT.I_SF_COUNTRY T4
      ON T1.NATN_CD = T4.Country_ISO_code__c 
    WHERE 1=1
      AND T1.CUSTR_CD NOT IN ('HB20172','HB20173','HB20164','HB20156') /* 2023-11-01 엄프로님 HOMS 중복등록 한 건으로 로직에서 제외시킴 */
	;

	-- 로그메시지 변경
    lv_log_msg := '002. INSERT';
    lv_row_cnt := SQLROWCOUNT ;
    SELECT TO_VARCHAR(CONVERT_TIMEZONE('Asia/Seoul', CURRENT_TIMESTAMP()),'YYYY-MM-DD HH24:MI:SS') INTO lv_step_end_dt ;
    CALL HDL.INT._SP_WRITE_LOG(:lv_call_in_info, :lv_job_type, 'PROCEDURE', :lv_proc_nm, :lv_step_str_dt, :lv_step_end_dt, 'SUCCESS', :lv_step_no, :lv_etl_type, :lv_base_date, :lv_row_cnt, :lv_log_msg);
    lv_log_msg_flow := lv_log_msg_flow||'>'||lv_log_msg;


    -- 003
    lv_step_no := '003';
    SELECT TO_VARCHAR(CONVERT_TIMEZONE('Asia/Seoul', CURRENT_TIMESTAMP()),'YYYY-MM-DD HH24:MI:SS') INTO lv_step_str_dt ;

    -- Logic
    USE DATABASE HDL;
    USE SCHEMA MRT;
	
	REMOVE @HDL_SALES_OUT_STAGE/M_SF_ACCOUNT/insert/;

    COPY INTO @HDL_SALES_OUT_STAGE/M_SF_ACCOUNT/insert/
    from(
        SELECT T1.AccountId                   AS ID
              ,T1.AccountName                 AS NAME
              ,T1.TYPE                 AS TYPE
              ,T1.RECORDTYPEID         AS RECORDTYPEID
              ,T1.CURRENCYISOCODE      AS CURRENCYISOCODE
              ,T1.OWNERID              AS OWNERID
              -- ,T1.SHORT_NAME__C        AS SHORT_NAME__C SHORT_NAME 제외처리 2023.11.07
              ,T1.INCOTERMS__C         AS INCOTERMS__C
              ,T1.COUNTRY__C           AS COUNTRY__C
              ,T1.REGION__C            AS REGION__C
              ,T1.HOMS_NO__C           AS HOMS_NO__C
              ,T1.ACC_TYPE__C          AS ACC_TYPE__C
              ,T1.ERP_NO__C            AS ERP_NO__C
              ,T1.Division_Code__c     AS Division_Code__c
              ,T1.DIVISION__C          AS DIVISION__C
              ,T1.CREATOR__C           AS CREATOR__C
              ,T1.MODIFIER__C          AS MODIFIER__C
			  ,to_char(T1.CREATED_DATE__C,'yyyy-mm-ddThh24:mi:ss.000+0000') AS CREATED_DATE__C
			  ,to_char(T1.MODIFIED_DATE__C,'yyyy-mm-ddThh24:mi:ss.000+0000') AS MODIFIED_DATE__C
              ,T1.CREATORIP__C         AS CREATORIP__C
              ,T1.MODIFIERIP__C        AS MODIFIERIP__C
			  ,to_char(T1.REGISTRATION_DATE__C,'yyyy-mm-ddThh24:mi:ss.000+0000') AS REGISTRATION_DATE__C
          FROM HDL.MRT.M_SF_ACCOUNT T1
          WHERE T1.AccountId IS NULL)
    OVERWRITE=TRUE
    HEADER = TRUE
    ;


	-- 로그메시지 변경
    lv_log_msg := '003. COPY(INSERT) : MRT -> S3';
	lv_row_cnt := SQLROWCOUNT ;
    SELECT TO_VARCHAR(CONVERT_TIMEZONE('Asia/Seoul', CURRENT_TIMESTAMP()),'YYYY-MM-DD HH24:MI:SS') INTO lv_step_end_dt ;
    CALL HDL.INT._SP_WRITE_LOG(:lv_call_in_info, :lv_job_type, 'PROCEDURE', :lv_proc_nm, :lv_step_str_dt, :lv_step_end_dt, 'SUCCESS', :lv_step_no, :lv_etl_type, :lv_base_date, :lv_row_cnt, :lv_log_msg);
    lv_log_msg_flow := lv_log_msg_flow||'>'||lv_log_msg;


    -- 004
    lv_step_no := '004';
    SELECT TO_VARCHAR(CONVERT_TIMEZONE('Asia/Seoul', CURRENT_TIMESTAMP()),'YYYY-MM-DD HH24:MI:SS') INTO lv_step_str_dt ;

    REMOVE @HDL_SALES_OUT_STAGE/M_SF_ACCOUNT/update/;
	
    -- Logic
    COPY INTO @HDL_SALES_OUT_STAGE/M_SF_ACCOUNT/update/
    from(
        SELECT T1.AccountId            AS ID
              ,T1.AccountName          AS NAME
              ,T1.TYPE                 AS TYPE
              ,T1.RECORDTYPEID         AS RECORDTYPEID
              ,T1.CURRENCYISOCODE      AS CURRENCYISOCODE
              ,T1.OWNERID              AS OWNERID
              -- ,T1.SHORT_NAME__C        AS SHORT_NAME__C SHORT_NAME 제외처리 2023.11.07
              ,T1.INCOTERMS__C         AS INCOTERMS__C
              ,T1.COUNTRY__C           AS COUNTRY__C
              ,T1.REGION__C            AS REGION__C
              ,T1.HOMS_NO__C           AS HOMS_NO__C
              ,T1.ACC_TYPE__C          AS ACC_TYPE__C
              ,T1.ERP_NO__C            AS ERP_NO__C
              ,T1.Division_Code__c     AS Division_Code__c
              ,T1.DIVISION__C          AS DIVISION__C
        	  ,T1.CREATOR__C           AS CREATOR__C
              ,T1.MODIFIER__C          AS MODIFIER__C
			  ,to_char(T1.CREATED_DATE__C,'yyyy-mm-ddThh24:mi:ss.000+0000') AS CREATED_DATE__C
			  ,to_char(T1.MODIFIED_DATE__C,'yyyy-mm-ddThh24:mi:ss.000+0000') AS MODIFIED_DATE__C
              ,T1.CREATORIP__C         AS CREATORIP__C
              ,T1.MODIFIERIP__C        AS MODIFIERIP__C
			  ,to_char(T1.REGISTRATION_DATE__C,'yyyy-mm-ddThh24:mi:ss.000+0000') AS REGISTRATION_DATE__C
           FROM HDL.MRT.M_SF_ACCOUNT T1
          WHERE 1=1 
            AND T1.AccountId IS NOT NULL
        	AND nvl(T1.MODIFIED_DATE__C,to_date('0001-01-01')) > (SELECT nvl(HOMS_LAST_JOB_DTM,to_date('1900-01-01'))           /* HOMS최종작업일시 */        
        	                             FROM HDL.INT.C_CM_JOB_DETAIL      /* C_CM_작업상세   */
        	                            WHERE TABLE_ID = 'M_SF_ACCOUNT'))
    OVERWRITE=TRUE
    HEADER = TRUE
    ;

	-- 로그메시지 변경
    lv_log_msg := '004. COPY(UPDATE) : MRT -> S3';
	lv_row_cnt := SQLROWCOUNT ;
    SELECT TO_VARCHAR(CONVERT_TIMEZONE('Asia/Seoul', CURRENT_TIMESTAMP()),'YYYY-MM-DD HH24:MI:SS') INTO lv_step_end_dt ;
    CALL HDL.INT._SP_WRITE_LOG(:lv_call_in_info, :lv_job_type, 'PROCEDURE', :lv_proc_nm, :lv_step_str_dt, :lv_step_end_dt, 'SUCCESS', :lv_step_no, :lv_etl_type, :lv_base_date, :lv_row_cnt, :lv_log_msg);
    lv_log_msg_flow := lv_log_msg_flow||'>'||lv_log_msg;

	
    --------------------------------------------------------------
    -- [02. 작업 종료]
    ----------------------------------------
    -- Write Log
    SELECT TO_VARCHAR(CONVERT_TIMEZONE('Asia/Seoul', CURRENT_TIMESTAMP()),'YYYY-MM-DD HH24:MI:SS')
	INTO lv_proc_end_dt ;

    lv_log_msg := '999. FINISH';
    lv_step_no := '999';
    CALL HDL.INT._SP_WRITE_LOG(:lv_call_in_info, :lv_job_type, 'PROCEDURE', :lv_proc_nm, :lv_proc_str_dt, :lv_proc_end_dt, 'SUCCESS', :lv_step_no, :lv_etl_type, :lv_base_date, :lv_row_cnt, :lv_log_msg);
    lv_log_msg_flow := lv_log_msg_flow||'>'||lv_log_msg;
    --------------------------------------------------------------

	COMMIT;
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;

        -- Write Log
        SELECT TO_VARCHAR(CONVERT_TIMEZONE('Asia/Seoul', CURRENT_TIMESTAMP()),'YYYY-MM-DD HH24:MI:SS')
	    INTO lv_proc_end_dt ;

        lv_log_status := 'FAIL';
        CASE WHEN (lv_log_msg_flow ='' OR lv_log_msg_flow IS NULL) THEN 
            lv_log_msg := '';
        ELSE lv_log_msg := lv_log_msg_flow||'>>999. ROLLBACK ';
        END;

        lv_log_msg := lv_log_msg||'>>>>>>>>>> '||SQLERRM;
        lv_step_no := '999';

        CALL HDL.INT._SP_WRITE_LOG(:lv_call_in_info, :lv_job_type, 'PROCEDURE', :lv_proc_nm, :lv_proc_str_dt, :lv_proc_end_dt, 'FAIL', :lv_step_no, :lv_etl_type, :lv_base_date, :lv_row_cnt, :lv_log_msg);
        COMMIT;
        
        RAISE;
END
$$
;

