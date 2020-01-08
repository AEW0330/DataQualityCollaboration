select
        *
       ,(case
          when (cast(year_of_birth as int) > 1930 and cast(year_of_birth as int) < 1999 ) then 'N'
          when year_of_birth is null or day_of_birth is null then 'Y'
          else 'Y'
        end) as Date_error
       , 'Y' as include_yn
  into BYUN_META_1M.dbo.Dev_provider
  from

(
select
       'csusermt' as tbnm
      ,ROW_NUMBER() over(order by newid()) as uniq_no
      ,USERID as userid	 --사용자ID
     ,(case
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
   from BYUN_SOURCE_DATA.dbo.CSUSERMT) v ;

--> [2019-10-28 23:41:14] 24,799 rows affected in 376 ms
