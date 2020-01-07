/*************************************************************************/
--  Assigment: DataQueen project
--  Description: Running script for Meta Person
--  Author: Junghyun E, Byun
--  Date:  7. May, 2019
--  Job name: Random sampling Original data (EHR)
--  Language: MSSQL
--  Target data: Meta
--  Certype: 2. 사망진단서 , 3. 사체검안서, 5. 사산 증명서, 16,Death Certificate
--  select * from DW.dbo.DW_CSCOMCDT
--      where MIDGCODE = 'MM007'
--          ORDER BY SMALLGCODE
--  select * from EMR_Table_info.dbo.netm1 where 테이블명 = 'MMCERMST';
/*************************************************************************/

-- 03. Dev_death


    select
        ROW_NUMBER() over(order by newid()) as Uniq_no
        ,d1.PATNO
        ,MEDDATE	  --진료/입원/수술(DSC)/응급실도착일시
        ,d1.DIEDATE    -- 사망시간
       , (case
            when d1.CERTYP = '2'
            then 'Death_certification'
            when d1.CERTYP = '3'
            then 'Cerificate_of_death_dead_body'
            when d1.CERTYP ='5'
            then 'Certificate_of_stillbirth'
            else null
         end ) Cf_type
        ,d1.DPLCTYPE	  -- 사망(사산)종류
        ,d1.AETCDTP	  -- 외인사종류
        ,d1.NSBIRTH	  -- 자연사산원인
        ,d1.DIDEADRS	  -- 직접사인
        ,d1.MIDDEADRS	-- 중간선행사인
        ,d1.PREDEADRS	-- 선행사인

        ,(case
            when cast(d1.DIEDATE as date) >= cast(p1.Btdt as date)
            then 'N'
            when cast(d1.DIEDATE as date) <= cast(getdate() as date)
            then 'N'
            else 'Y'
           end
             ) as Date_error
    into Byun_meta_2M.dbo.Dev_death
    from
         (select * from  Byun_origin_Rand_2M.dbo.Death_cerification where Seq= '1') as d1
        join Byun_meta_2M.dbo.Dev_person as p1 on p1.PATNO = d1.PATNO ;
            -- [2019-05-07 20:46:21] 19290 rows affected in 1 s 281 ms

    -- 비고: 메타에서 Rownum 생성 시 Death certification만 가져오게 수정 필요


