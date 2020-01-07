/*************************************************************************/
--  Assigment: DataQueen project
--  Description: Running script for Meta Person
--  Author: Junghyun E, Byun
--  Date:  7. May, 2019
--  Job name: Random sampling Original data (EHR)
--  Language: MSSQL
--  Target data: Meta

--  B.DRST in ('1','2','3','4','5') THEN  4010577  --(Medical doctor)
--  WHEN B.DRST ='6' THEN  4010474 --(Practice nurse)
--	ELSE 38004698	 -- (All Other Suppliers)
-- 생년월일 체크 코드 수정 필요
/*************************************************************************/

select
        *
       ,(case
          when RESNO1 is not null
                  and (cast(year_of_birth as int) > 1930 and cast(year_of_birth as int) < 1999 )
          then 'N'
          when RESNO1 is null
          then 'Y'
          else 'Y'
        end) as Date_error
  into Byun_meta_2M.dbo.Dev_provider
  from

(
select
      row_number()over(partition by EMPNO order by EMPNO) as seq
      ,USERID	, --사용자ID
      substring(RESNO,1,6)  as RESNO1	, --주민등록번호
      (case
          when substring(RESNO,7,1) = '0'
                or substring(RESNO,7,1) = '9'
            then concat('18',substring(RESNO,1,2))
          when substring(RESNO,7,1) = '3'
                or substring(RESNO,7,1) = '4'
            then concat('20',substring(RESNO,1,2))
          else concat('19',substring(RESNO,1,2))
       end) as year_of_birth
      ,substring(RESNO,3,2)+'-'+substring(RESNO,5,2) as day_of_birth
      ,substring(RESNO,7,1) as RESNO2
      ,SEX	 --성별
      ,EMPNO	 --사번
      ,USERSTAT	 --사용상태(CS001)
      ,DEPTCODE	 --부서코드(OCS작업부서)
      ,(case
          when DRST in ('1','2','3','4','5')
            then 'DR'
          when DRST = '6'
            then 'RN'
        Else 'Other_supplier'
        end) as Position
      ,(case
          when DRDIV = '0'
            then 'Cant'
          when DRDIV = '1'
            then 'Order'
          else null
        end ) Order_GB
   from DW.dbo.DW_CSUSERMT) v ;
-- [2019-05-08 10:49:37] 24,799 rows affected in 293 ms


  select * from DW.dbo.DW_CSCOMCDT
      where MIDGCODE = 'CS001'
          ORDER BY SMALLGCODE