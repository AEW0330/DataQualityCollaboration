# # source(file.path(getwd(),'R/Full_script.R'))
if (!require('shiny')){install.packages('shiny'); library(shiny)}
if (!require('shinyjs')){install.packages('shinyjs'); library(shinyjs)}
if (!require('highcharter')){install.packages('highcharter'); library(highcharter)}
if (!require('treemap')){install.packages('treemap'); library(treemap)}
if (!require('DT')){install.packages('DT'); library(DT)}
if (!require('xts')){install.packages('xts'); library(xts)}
if (!require('dplyr')){install.packages('dplyr'); library(dplyr)}
if (!require('dygraphs')){install.packages('dygraphs'); library(dygraphs)}
if (!require('lubridate')){install.packages('lubridate'); library(lubridate)}
if (!require('plotly')){install.packages('plotly'); library(plotly)}
if (!require('ECharts2Shiny')){install.packages('ECharts2Shiny'); library(ECharts2Shiny)}
if (!require('shinythemes')){install.packages('shinythemes'); library(shinythemes)}
if (!require('visNetwork')){install.packages('visNetwork'); library(visNetwork)}
if (!require('dplyr')){install.packages('dplyr'); library(dplyr)}
if (!require('ggplot2')){install.packages('ggplot2'); library(ggplot2)}
if (!require('ggiraph')){install.packages('ggiraph'); library(ggiraph)}
if (!require('reshape')){install.packages('reshape'); library(reshape)}
if (!require('reshape2')){install.packages('reshape2'); library(reshape2)}
if (!require('dplyr')){install.packages('dplyr'); library(dplyr)}
if (!require('shinyBS')){install.packages('shinyBS'); library(shinyBS)}
# #example query
# #Database Connect
ConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sql server",
                                                                server = "",  #IP
                                                                schema = "",
                                                                user = "",        #User id
                                                                password = "")

connection <- DatabaseConnector::connect(ConnectionDetails)

resultDB <- 'DQUEEN_Result.dbo'
totalScore <- 81.85
metaMainScore <- 82.4
metaPersonScore <- 91
metaVisitScore <- 72.4

cdmMainScore   <- 81.3
cdmPersonScore <- 94
cdmVisitScore  <- 84



metaMainValidation <- c(82,80,90,95,90)
metaPersonValidation <- c(92,85,92,100,100)
metaVisitValidation <- c(72,75,88,90,80)

cdmMainValidation <- c(75,85,86,90,90)
cdmPersonValidation <- c(91,90,90,100,98)
cdmVisitValidation <- c(86,83,78,80,90)

types=c('S','M','C')
threshold = 100 # define not Available.
for(type in types){
  sql <- SqlRender::readSql(file.path(getwd(),'inst/sql/TableSize.sql'))
  sql <- SqlRender::renderSql(sql
                              ,resultDB = resultDB
                              ,type = type
  )$sql
  sql <- SqlRender::translateSql(sql,
                                 targetDialect=ConnectionDetails$dbms)$sql

  tableSize <- DatabaseConnector::dbGetQuery(connection,sql)

  threshold_df <- data.frame('status' = tableSize$rows < threshold,'index' = rep(1,nrow(tableSize)))
  tableSize <- cbind(tableSize,threshold_df)
  tmpStatus <- gsub('FALSE','Avaliable',tableSize$status)
  tmpStatus <- gsub('TRUE','not Avaliable',tmpStatus)
  tableSize$status = tmpStatus


  switch(type,'S'={sourceTableSize <- tableSize}
         ,'M'={metaTableSize <- tableSize}
         ,'C'={cdmTableSize <- tableSize})

}

query <- "SELECT * FROM DQUEEN_Result.dbo.data_1"
personCount <- DatabaseConnector::dbGetQuery(connection,query)
personCount <- reshape2::melt(personCount)
personCount <- reshape::cast(data = personCount,stage ~ SEX)
rownames(personCount) <- personCount$stage
personCount <- personCount[-1]


query <- "SELECT * FROM DQUEEN_Result.dbo.data_2"
metaObservationPeriod <- DatabaseConnector::dbGetQuery(connection,query)
metaObservationPeriod <- na.omit(metaObservationPeriod)
metaObservationPeriod <- plyr::arrange(metaObservationPeriod,cnt_yy)
rownames(metaObservationPeriod) <- metaObservationPeriod$cnt_yy
metaObservationPeriod <- metaObservationPeriod[-1]

sql <- "SELECT * FROM DQUEEN_Result.dbo.data_3"
cdmObservationPeriod <- DatabaseConnector::dbGetQuery(connection,query)
cdmObservationPeriod <- na.omit(cdmObservationPeriod)
cdmObservationPeriod <- plyr::arrange(cdmObservationPeriod,cnt_yy)
rownames(cdmObservationPeriod) <- cdmObservationPeriod$cnt_yy
cdmObservationPeriod <- cdmObservationPeriod[-1]


observationPeriod <- cbind(metaObservationPeriod,cdmObservationPeriod)
colnames(observationPeriod) <- c('Meta','CDM')




query<-"SELECT * FROM DQUEEN_Result.dbo.data_4 order by patfg"
visitLength <- DatabaseConnector::dbGetQuery(connection,query)

query <- "SELECT * FROM DQUEEN_Result.dbo.data_5"
visitLengthStatic <- DatabaseConnector::dbGetQuery(connection,query)

scoreColorFunction <- function(score){
  if(score >= 80){
    scoreColor = 'Green'
  }
  else if(score < 80 & score >= 50){
    scoreColor = 'Orange'
  }
  else{
    scoreColor = 'Red'
  }
  return(scoreColor)
}

donutPlot <- function(score){
  scoreDonut <- data.frame(type = c("blank", 'score'), value = c(100-score, score)) %>%
    mutate(
      percentage = value / sum(value),
      hover_text = paste0(type, ": ", value)
    ) %>%
    mutate(percentage_label = paste0(round(100 * percentage, 1), "%"))
  return(scoreDonut)
}


donutPlotNoPer <- function(score){
  scoreDonut <- data.frame(type = c("blank", 'score'), value = c(100-score, score)) %>%
    mutate(
      percentage = value / sum(value),
      hover_text = paste0(type, ": ", value)
    ) %>%
    mutate(percentage_label = paste0(round(100 * percentage, 1), ""))
  return(scoreDonut)
}


drawDonutPlot <- function(scoreDonut,scoreColor){
  scoreDonutPlot <- ggplot(scoreDonut, aes(y = value, fill = type)) +
    geom_bar_interactive(
      aes(x = 1, tooltip = hover_text),
      width = 0.1,
      stat = "identity",
      show.legend = FALSE
    ) +
    ggplot2::annotate(
      geom = "text",
      x = 0,
      y = 0,
      label = scoreDonut[["percentage_label"]][scoreDonut[["type"]] == "score"],
      size = 20,
      color = scoreColor
    ) +
    scale_fill_manual(values = c(blank = "#ede4e4", score = scoreColor)) +
    coord_polar(theta = "y") +
    theme_void()
  return(scoreDonutPlot)
}

query <- "SELECT * FROM DQUEEN_Result.dbo.data_6"
visitPerson <- DatabaseConnector::dbGetQuery(connection,query)

query <- "select
distinct
stratum1 as TABLE_NAME
,stratum2 as visit_type
,stratum3 as count_year
,cast(count_val as bigint) as count_val
from DQUEEN_Result.dbo.dq_result where check_id = 19 and (stratum3 > 1993 or stratum3 <2018) and stratum4 is not null
union all
select
distinct
stratum1 as TABLE_NAME
,stratum2+'_visit' as visit_type
,stratum3 as count_year
,cast(stratum4 as bigint) as count_val
from DQUEEN_Result.dbo.dq_result where check_id = 19 and (stratum3 > 1993 or stratum3 <2018) and stratum4 is not null"
visitType <- DatabaseConnector::dbGetQuery(connection,query)



query <- "SELECT * FROM DQUEEN_Result.dbo.data_7"
PatientClassification <- DatabaseConnector::dbGetQuery(connection,query)
PatientClassification <- reshape2::melt(PatientClassification)
PatientClassification <- reshape::cast(data = PatientClassification,SEX ~ exclude_gb)
rownames(PatientClassification) <- PatientClassification$SEX
PatientClassification <- PatientClassification[-1]

query <- "SELECT * FROM DQUEEN_Result.dbo.data_8 order by yy"
personBirth <- DatabaseConnector::dbGetQuery(connection,query)
rownames(personBirth) <- personBirth$'yy'
personBirth <- personBirth[-1]


query <- "SELECT * FROM DQUEEN_Result.dbo.data_9"
visitValidation <- DatabaseConnector::dbGetQuery(connection,query)

query <- "select
distinct
stratum1 as tbnm
,stratum2 as patfg
,stratum3 as visit_year
,count_val
,P_25
from DQUEEN_Result.dbo.dq_result_statics where check_id = 19 and stratum1 = 'Dev_visit'"
visitData <- DatabaseConnector::dbGetQuery(connection,query)


dat_2 <- data.frame(c(1, 2, 3, 1,NA),
                    c(2, 4, 6, 6,NA),
                    c(3, 2, 7, 5, 5))
names(dat_2) <- c("Type-A", "Type-B", "Type-C")
row.names(dat_2) <- c("Time-1", "Time-2", "Time-3", "Time-4",'Time-5')


dat <- data.frame(Meta = c(72,82,40,50,54),
                  CDM = c(75,78,35,55,59)
)
row.names(dat) <- c("Plausibility", "Accuracy", "Completeness", "Conformance", "Consistency")

#error pie Chart
errorNum <- c(rep("Plausibility", 179),
              rep("Accuracy", 268),
              rep("Completeness", 214),
              rep("Conformance", 256),
              rep("Consistency", 211))


errorNum2 <- c(rep("Plausibility", metaValidation[1]),
               rep("Accuracy", metaValidation[2]),
               rep("Completeness", metaValidation[3]),
               rep("Conformance", metaValidation[4]),
               rep("Consistency", metaValidation[5]))


#Person Count


#Rader
metaErrorValidationData <- data.frame(META = metaMainValidation,
                                      'Perfect score' = rep(100,5))
row.names(metaErrorValidationData) <- c("Plausibility", "Accuracy", "Completeness", "Conformance", "Consistency")

cdmErrorValidationData <- data.frame(CDM = cdmMainValidation,
                                     'Perfect score' = rep(100,5))
row.names(cdmErrorValidationData) <- c("Plausibility", "Accuracy", "Completeness", "Conformance", "Consistency")



#visNetwork
metaTableSize$TABLE_NAME <- tolower(metaTableSize$TABLE_NAME)
sourceTableSize$TABLE_NAME <- tolower(sourceTableSize$TABLE_NAME)
cdmTableSize$TABLE_NAME <- tolower(cdmTableSize$TABLE_NAME)

myTable <- read.csv(file.path(getwd(),'inst/csv/schemas/source_mapped_origin_source2meta.csv'),stringsAsFactors = F)
myTable$META_TABLE <- tolower(myTable$META_TABLE)
myTable$SOURCE_TABLE <- tolower(myTable$SOURCE_TABLE)

myTable2 <- read.csv(file.path(getwd(),'inst/csv/schemas/source_mapped_origin_meta2cdm.csv'),stringsAsFactors = F)
myTable2$META_TABLE <- tolower(myTable2$META_TABLE)
myTable2$CDM_TABLE <- tolower(myTable2$CDM_TABLE)


#MetaNodes
metaNodes <- data.frame(id = unique(myTable$META_TABLE),label = unique(myTable$META_TABLE),group = 'meta')
metaNodesValue <- merge(metaNodes,metaTableSize,by.x='id',by.y='TABLE_NAME')
metaNodesValue <- select(metaNodesValue,'id','label','group','rows')
metaNodes <- merge(metaNodes,metaNodesValue,all.x = T)
#sourceNodes
sourceNodes <- data.frame(id = unique(myTable$SOURCE_TABLE),label = unique(myTable$SOURCE_TABLE),group = 'source')
sourceNodesValue <- merge(sourceNodes,sourceTableSize,by.x='id',by.y='TABLE_NAME')
sourceNodesValue <- select(sourceNodesValue,'id','label','group','rows')
sourceNodes <- merge(sourceNodes,sourceNodesValue,all.x = T)
#cdmNodes
cdmNodes <- data.frame(id = unique(myTable2$CDM_TABLE),label = unique(myTable2$CDM_TABLE),group = 'cdm')
cdmNodesValue <- merge(cdmNodes,cdmTableSize,by.x='id',by.y='TABLE_NAME')
cdmNodesValue <- select(cdmNodesValue,'id','label','group','rows')
cdmNodes <- merge(cdmNodes,cdmNodesValue,all.x = T)

nodes <- rbind(metaNodes,sourceNodes,cdmNodes)

edges <- data.frame(from = myTable$META_TABLE, to = myTable$SOURCE_TABLE)
edges2 <- data.frame(from = myTable2$CDM_TABLE, to = myTable2$META_TABLE)
#edges <- rbind(edges,edges2)

edges <- merge(edges,metaNodes,by.x ='from',by.y='id',all.x = T) %>% select(from,to,rows) %>% 'colnames<-'(c("from", "to", "metaRows"))
edges <- merge(edges,sourceNodes,by.x ='to',by.y='id',all.x = T) %>% select(from,to,metaRows,rows) %>% 'colnames<-'(c("from", "to", "metaRows","sourceRows"))
edges$cdmRows <- NA

edges2 <- merge(edges2,metaNodes,by.x ='to',by.y='id',all.x = T) %>% select(from,to,rows) %>% 'colnames<-'(c("from", "to", "metaRows"))
edges2 <- merge(edges2,cdmNodes,by.x ='from',by.y='id',all.x = T) %>% select(from,to,metaRows,rows) %>% 'colnames<-'(c("from", "to", "metaRows","cdmRows"))
edges2$sourceRows <- NA

edges <- rbind(edges,edges2)


#error Table###############################################################
sql <- "SELECT * FROM @resultDB.dq_result WHERE stratum2 like '%error%';"
sql <- SqlRender::renderSql(sql,resultDB = resultDB)$sql
sql <- SqlRender::translateSql(sql,
                               targetDialect=ConnectionDetails$dbms)$sql
metaerror <- DatabaseConnector::dbGetQuery(connection,sql)
#error Table###############################################################
#
#output
CheckLevel = 'Lv.3'
DqScore = 80
DataMetaPopulation = 'Meta'
DataCdmPopulation = 'CDM'
DataStatus = 'good'
metaPopulation <- 1774392
cdmPopulation <- 1749035
DataPeriod = '1994.09.12~2017.12.31'
DataCapacity = '89 GB'


cdmMetaCapacityData <- c(rep('Meta',67),rep('CDM',34))

# #Regularation code
# # shiny::runApp(appDir = system.file("examples/shiny/crimes", package = "ggiraph"), display.mode = "showcase")






query<- "select
v1.*
,v2.*
from DQUEEN_Result.dbo.data_10 as v1
left join
(select distinct
patfg
,cast(avg(daily_visit_cnt)as float) as avg_val
,cast(stdev(daily_visit_cnt)as float) as stdev_val
,min(daily_visit_cnt) as min_val
,max(daily_visit_cnt) as max_val
from DQUEEN_Result.dbo.data_10
group by patfg) as v2
on v1.patfg = v2.patfg
order by v1.patfg, v1.daily_visit_cnt;"
daliyVisitType <- DatabaseConnector::dbGetQuery(connection,query)

query <- "select distinct
v2.*
from DQUEEN_Result.dbo.data_11 as v1
left join
(select distinct
patfg
,cast(avg(daily_visit_cnt)as float) as avg_val
,cast(stdev(daily_visit_cnt)as float) as stdev_val
,min(daily_visit_cnt) as min_val
,max(daily_visit_cnt) as max_val
from DQUEEN_Result.dbo.data_11
group by patfg) as v2
on v1.patfg = v2.patfg"

daliyVisitInfo <- DatabaseConnector::dbGetQuery(connection,query)

query <- "select rm1.*,r2.col_count,r3.uniq_count,r4.missing_count,r5.special_char_count from
(select
m1.TABLE_NAME
,r1.col
,m1.rows
,r1.dist_count
from (select TABLE_NAME, rows from DQUEEN_Result.dbo.schema_capacity
where stage_gb = 'M' and TABLE_NAME = 'Dev_person') as m1
inner join
(select
stratum1 as tbnm
,stratum2 as col
,count_val as dist_count
from DQUEEN_Result.dbo.dq_result
where check_id = 1 and stratum1 = 'Dev_person') as r1
on m1.TABLE_NAME = r1.tbnm) as rm1
left join
(select
stratum1 as tbnm
,stratum2 as col
,count_val as col_count
from DQUEEN_Result.dbo.dq_result
where check_id = 2 and stratum1 = 'Dev_person') as r2
on rm1.TABLE_NAME = r2.tbnm and rm1.col = r2.col
left join
(select
stratum1 as tbnm
,stratum2 as col
,count_val as uniq_count
from DQUEEN_Result.dbo.dq_result
where check_id = 3 and stratum1 = 'Dev_person') as r3
on rm1.TABLE_NAME = r3.tbnm and rm1.col = r3.col
left join
(select
stratum1 as tbnm
,stratum2 as col
,count_val as missing_count
from DQUEEN_Result.dbo.dq_result
where check_id = 4 and stratum1 = 'Dev_person') as r4
on rm1.TABLE_NAME = r4.tbnm and rm1.col = r4.col
left join
(select
stratum1 as tbnm
,stratum2 as col
,count_val as special_char_count
from DQUEEN_Result.dbo.dq_result
where check_id = 5 and stratum1 = 'Dev_person') as r5
on rm1.TABLE_NAME = r5.tbnm and rm1.col = r5.col"

metaPersonInfo <- DatabaseConnector::dbGetQuery(connection,query)

query <- "select rm1.*,r2.col_count,r3.uniq_count,r4.missing_count,r5.special_char_count from
(select
m1.TABLE_NAME
,r1.col
,m1.rows
,r1.dist_count
from (select TABLE_NAME, rows from DQUEEN_Result.dbo.schema_capacity
where stage_gb = 'M' and TABLE_NAME = 'Dev_visit_detail') as m1
inner join
(select
stratum1 as tbnm
,stratum2 as col
,count_val as dist_count
from DQUEEN_Result.dbo.dq_result
where check_id = 1 and stratum1 = 'Dev_visit_detail') as r1
on m1.TABLE_NAME = r1.tbnm) as rm1
left join
(select
stratum1 as tbnm
,stratum2 as col
,count_val as col_count
from DQUEEN_Result.dbo.dq_result
where check_id = 2 and stratum1 = 'Dev_visit_detail') as r2
on rm1.TABLE_NAME = r2.tbnm and rm1.col = r2.col
left join
(select
stratum1 as tbnm
,stratum2 as col
,count_val as uniq_count
from DQUEEN_Result.dbo.dq_result
where check_id = 3 and stratum1 = 'Dev_visit_detail') as r3
on rm1.TABLE_NAME = r3.tbnm and rm1.col = r3.col
left join
(select
stratum1 as tbnm
,stratum2 as col
,count_val as missing_count
from DQUEEN_Result.dbo.dq_result
where check_id = 4 and stratum1 = 'Dev_visit_detail') as r4
on rm1.TABLE_NAME = r4.tbnm and rm1.col = r4.col
left join
(select
stratum1 as tbnm
,stratum2 as col
,count_val as special_char_count
from DQUEEN_Result.dbo.dq_result
where check_id = 5 and stratum1 = 'Dev_visit_detail') as r5
on rm1.TABLE_NAME = r5.tbnm and rm1.col = r5.col"

metaVisitInfo <- DatabaseConnector::dbGetQuery(connection,query)



types = c('M','C')
for(type in types){
  sql <- SqlRender::readSql(file.path(getwd(),'inst/sql/scorePiePlot.sql'))
  sql <- SqlRender::renderSql(sql
                              ,resultDB = resultDB
                              ,type = type
  )$sql
  sql <- SqlRender::translateSql(sql,
                                 targetDialect=ConnectionDetails$dbms)$sql

  piePlot <- DatabaseConnector::dbGetQuery(connection,sql)

  switch(type,'M'={metaPiePlot <- piePlot}
         ,'C'={cdmPiePlot<- piePlot})
}


sql <- SqlRender::readSql(file.path(getwd(),'inst/sql/M_mainHist.sql'))
sql <- SqlRender::renderSql(sql
                            ,resultDB = resultDB
)$sql
sql <- SqlRender::translateSql(sql,
                               targetDialect=ConnectionDetails$dbms)$sql

metaMainHist <- DatabaseConnector::dbGetQuery(connection,sql)


sql<- "select
*
from
DQUEEN_Result.dbo.data_12"
cdmPiePlot <- DatabaseConnector::dbGetQuery(connection,sql)

sql <- "select
category
,CDM_TABLE_NAME as stratum1
,count(*) as count_val
from DQUEEN_Result.dbo.dqdashboard_results
where FAILED = 1
group by category,CDM_TABLE_NAME"
cdmMainHist <- DatabaseConnector::dbGetQuery(connection,sql)


sql <- "
select
case
	when stage_gb = 'C' then 'CDM'
	when stage_gb = 'M' then 'Meta'
	end as stage_gb
	, (sum(table_size)/1024)/1024 as volumn from DQUEEN_Result.dbo.schema_capacity
where Stage_gb in ('M','C')
group by stage_gb"
dbVolumn <- DatabaseConnector::dbGetQuery(connection,sql)


sql <- "select * from DQUEEN_Result.dbo.data_13"
cdmMessageBox <- DatabaseConnector::dbGetQuery(connection,sql)

sql <- "select * from DQUEEN_Result.dbo.data_14"
metaMessageBox <- DatabaseConnector::dbGetQuery(connection,sql)

sql <- "
select
   rule_id
  ,category
  ,sub_category
  ,dq_msg
  ,count_val
  ,check_id
from
(select dm1.*, dr1.stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id not in (31,53,30,39,1,2,3,20)
union all
select distinct  dm1.*, 'Dev_measurement'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  30
union all
select distinct  dm1.*, 'Dev_condition'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  31
union all
select distinct  dm1.*, 'Dev_visit_detail'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  53
union all
select distinct  dm1.*, 'Dev_condition'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  54
union all
select distinct dm1.*, 'Dev_note' as stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id = 20)v
  where stratum1 = 'Dev_person'"
metaPersonMessageBox <- DatabaseConnector::dbGetQuery(connection,sql)

sql <- "
select
    category
  -- ,sub_category
   ,stratum1
   ,count(*) as count_val

from
(select dm1.*, dr1.stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id not in (31,53,30,39,1,2,3,20)
union all
select distinct  dm1.*, 'Dev_measurement'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  30
union all
select distinct  dm1.*, 'Dev_condition'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  31
union all
select distinct  dm1.*, 'Dev_visit_detail'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  53
union all
select distinct  dm1.*, 'Dev_condition'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  54
union all
select distinct dm1.*, 'Dev_note' as stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id = 20)v
where stratum1 = 'Dev_person'
group by category,stratum1
order by category, stratum1;
"
metaPersonPiePlot <- DatabaseConnector::dbGetQuery(connection,sql)

sql <- "
select
    category
   ,sub_category as stratum1
   ,count(*) as count_val

from
(select dm1.*, dr1.stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id not in (31,53,30,39,1,2,3,20)
union all
select distinct  dm1.*, 'Dev_measurement'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  30
union all
select distinct  dm1.*, 'Dev_condition'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  31
union all
select distinct  dm1.*, 'Dev_visit_detail'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  53
union all
select distinct  dm1.*, 'Dev_condition'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  54
union all
select distinct dm1.*, 'Dev_note' as stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id = 20)v
where stratum1 = 'Dev_person'
group by category,sub_category
order by category,sub_category;
"
metaPersonHist <- DatabaseConnector::dbGetQuery(connection,sql)


sql <- "select
category
--,sub_category
,stratum1
,count(*) as count_val

from
(select dm1.*, dr1.stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
             where count_val > 0) as dr1
  on dm1.check_id = dr1.check_id
  where dm1.count_val > 0 and dm1.rule_id not in (31,53,30,39,1,2,3,20)
  union all
  select distinct  dm1.*, 'Dev_measurement'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
             where count_val > 0) as dr1
  on dm1.check_id = dr1.check_id
  where dm1.count_val > 0 and dm1.rule_id =  30
  union all
  select distinct  dm1.*, 'Dev_condition'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
             where count_val > 0) as dr1
  on dm1.check_id = dr1.check_id
  where dm1.count_val > 0 and dm1.rule_id =  31
  union all
  select distinct  dm1.*, 'Dev_visit_detail'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
             where count_val > 0) as dr1
  on dm1.check_id = dr1.check_id
  where dm1.count_val > 0 and dm1.rule_id =  53
  union all
  select distinct  dm1.*, 'Dev_condition'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
             where count_val > 0) as dr1
  on dm1.check_id = dr1.check_id
  where dm1.count_val > 0 and dm1.rule_id =  54
  union all
  select distinct dm1.*, 'Dev_note' as stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
             where count_val > 0) as dr1
  on dm1.check_id = dr1.check_id
  where dm1.count_val > 0 and dm1.rule_id = 20)v
where stratum1 = 'Dev_visit_detail'
group by category,stratum1
order by category, stratum1;"
metaVisitPiePlot <- DatabaseConnector::dbGetQuery(connection,sql)

sql <- "select
category
,sub_category as stratum1
,count(*) as count_val

from
(select dm1.*, dr1.stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
             where count_val > 0) as dr1
  on dm1.check_id = dr1.check_id
  where dm1.count_val > 0 and dm1.rule_id not in (31,53,30,39,1,2,3,20)
  union all
  select distinct  dm1.*, 'Dev_measurement'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
             where count_val > 0) as dr1
  on dm1.check_id = dr1.check_id
  where dm1.count_val > 0 and dm1.rule_id =  30
  union all
  select distinct  dm1.*, 'Dev_condition'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
             where count_val > 0) as dr1
  on dm1.check_id = dr1.check_id
  where dm1.count_val > 0 and dm1.rule_id =  31
  union all
  select distinct  dm1.*, 'Dev_visit_detail'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
             where count_val > 0) as dr1
  on dm1.check_id = dr1.check_id
  where dm1.count_val > 0 and dm1.rule_id =  53
  union all
  select distinct  dm1.*, 'Dev_condition'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
             where count_val > 0) as dr1
  on dm1.check_id = dr1.check_id
  where dm1.count_val > 0 and dm1.rule_id =  54
  union all
  select distinct dm1.*, 'Dev_note' as stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
             where count_val > 0) as dr1
  on dm1.check_id = dr1.check_id
  where dm1.count_val > 0 and dm1.rule_id = 20)v
where stratum1 = 'Dev_visit_detail'
group by category,sub_category
order by category, sub_category;"
metaVisitHist <- DatabaseConnector::dbGetQuery(connection,sql)

sql <- "
select
   rule_id
  ,category
  ,sub_category
  ,dq_msg
  ,count_val
  ,check_id
from
(select dm1.*, dr1.stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id not in (31,53,30,39,1,2,3,20)
union all
select distinct  dm1.*, 'Dev_measurement'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  30
union all
select distinct  dm1.*, 'Dev_condition'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  31
union all
select distinct  dm1.*, 'Dev_visit_detail'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  53
union all
select distinct  dm1.*, 'Dev_condition'stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id =  54
union all
select distinct dm1.*, 'Dev_note' as stratum1 from DQUEEN_Result.dbo.dq_result_summary_main as dm1
  left join (select distinct  check_id, stratum1 from DQUEEN_Result.dbo.dq_result
              where count_val > 0) as dr1
    on dm1.check_id = dr1.check_id
    where dm1.count_val > 0 and dm1.rule_id = 20)v
  where stratum1 = 'Dev_visit_detail'
"
metaVisitMessageBox <- DatabaseConnector::dbGetQuery(connection,sql)

sql <- "select * from DQUEEN_Result.dbo.data_15"
cdmPersonPiePlot <- DatabaseConnector::dbGetQuery(connection,sql)

sql <- "select * from DQUEEN_Result.dbo.data_16"
cdmPersonHist <- DatabaseConnector::dbGetQuery(connection,sql)


sql <- "select
   category
  --,SUBCATEGORY
  ,count(*) as count_val
from DQUEEN_Result.dbo.dqdashboard_results
  where FAILED = 1 and CDM_TABLE_NAME like 'VISIT%'
group by CATEGORY
"
cdmVisitPiePlot <- DatabaseConnector::dbGetQuery(connection,sql)

sql <- "select
   category
  ,SUBCATEGORY as stratum1
  ,count(*) as count_val
from DQUEEN_Result.dbo.dqdashboard_results
  where FAILED = 1 and CDM_TABLE_NAME like 'VISIT%'
group by CATEGORY,SUBCATEGORY"
cdmVisitHist <- DatabaseConnector::dbGetQuery(connection,sql)

sql <- "
select * from
(select
   CATEGORY
  ,SUBCATEGORY
  ,CHECK_DESCRIPTION
  ,CDM_TABLE_NAME
  ,CDM_FIELD_NAME
  ,count(*) as count_val
from DQUEEN_Result.dbo.dqdashboard_results
  where FAILED = 1 and CDM_TABLE_NAME = 'PERSON'
group by CATEGORY,SUBCATEGORY,CHECK_DESCRIPTION,CDM_TABLE_NAME,CDM_FIELD_NAME)v
  order by newid();
"
cdmPersonMessageBox <- DatabaseConnector::dbGetQuery(connection,sql)

sql <- "
select * from
(select
   CATEGORY
  ,SUBCATEGORY
  ,CHECK_DESCRIPTION
  ,CDM_TABLE_NAME
  ,CDM_FIELD_NAME
  ,count(*) as count_val
from DQUEEN_Result.dbo.dqdashboard_results
  where FAILED = 1 and CDM_TABLE_NAME like 'VISIT%'
group by CATEGORY,SUBCATEGORY,CHECK_DESCRIPTION,CDM_TABLE_NAME,CDM_FIELD_NAME)v
  order by newid();
"
cdmVisitMessageBox <- DatabaseConnector::dbGetQuery(connection,sql)





query <- "select
 yc1.TABLE_NAME
,yc1.count_year
,yc1.count_val
,yc2.visit_count
    from
(select
   stratum1 as TABLE_NAME
  ,stratum2 as count_year
  ,count_val
from DQUEEN_Result.dbo.dq_result where check_id = 72 and stratum4 = 'none') as yc1
left join
    (select stratum2 as visit_yy, count_val as visit_count
          from DQUEEN_Result.dbo.dq_result
               where check_id = 72 and stratum1 = 'visit_detail'
             and (stratum2 > 1993 and stratum2 < 2018) ) as yc2
on yc1.count_year = yc2.visit_yy"
cdmVisitPerson <- DatabaseConnector::dbGetQuery(connection,query)

query <- "select
stratum1 as TABLE_NAME
,case
when stratum2 = 9203 then 'Emergency Room Visit'
when stratum2 = 9202 then 'Outpatient Visit'
ELSE 'Inpatient Visit'
END as visit_type
,stratum3 as count_year
,cast(count_val as bigint) as count_val
from DQUEEN_Result.dbo.dq_result where check_id = 73 and (stratum3 > 1993 or stratum3 <2018) and stratum4 is not null
union all
select
distinct
'Dev_visit' as TABLE_NAME
,stratum2+'_visit' as visit_type
,stratum3 as count_year
,cast(stratum4 as bigint)as count_val
from DQUEEN_Result.dbo.dq_result where check_id = 73 and (stratum3 > 1993 or stratum3 <2018) and stratum4 is not null order by visit_type,count_year"
cdmVisitType  <- DatabaseConnector::dbGetQuery(connection,query)

query <- "select * from DQUEEN_Result.dbo.data_17"
cdmPatientClassification <- DatabaseConnector::dbGetQuery(connection,query)
cdmPatientClassification <- reshape2::melt(cdmPatientClassification)
cdmPatientClassification <- reshape::cast(data = cdmPatientClassification,SEX ~ exclude_gb)
rownames(cdmPatientClassification) <- cdmPatientClassification$SEX
cdmPatientClassification <- cdmPatientClassification[-1]



query <- "
select
*
from
DQUEEN_Result.dbo.data_18
order by YY
"
cdmPersonBirth <- DatabaseConnector::dbGetQuery(connection,query)
rownames(cdmPersonBirth) <- cdmPersonBirth$'yy'
cdmPersonBirth <- cdmPersonBirth[-1]



query <- "
select rm1.*,r2.col_count,r3.uniq_count,r4.missing_count,r5.special_char_count from
 (select
       m1.TABLE_NAME
      ,r1.col
      ,m1.rows
      ,r1.dist_count
  from (select TABLE_NAME, rows from DQUEEN_Result.dbo.schema_capacity
    where stage_gb = 'C' and TABLE_NAME = 'person') as m1
 inner join
  (select
     stratum1 as tbnm
    ,stratum2 as col
    ,count_val as dist_count
from DQUEEN_Result.dbo.dq_result
  where check_id = 74 and stratum1 = 'person') as r1
  on m1.TABLE_NAME = r1.tbnm) as rm1
 left join
  (select
     stratum1 as tbnm
    ,stratum2 as col
    ,count_val as col_count
from DQUEEN_Result.dbo.dq_result
  where check_id = 75 and stratum1 = 'person') as r2
  on rm1.TABLE_NAME = r2.tbnm and rm1.col = r2.col
left join
  (select
     stratum1 as tbnm
    ,stratum2 as col
    ,count_val as uniq_count
from DQUEEN_Result.dbo.dq_result
  where check_id = 76 and stratum1 = 'person') as r3
  on rm1.TABLE_NAME = r3.tbnm and rm1.col = r3.col
left join
  (select
     stratum1 as tbnm
    ,stratum2 as col
    ,count_val as missing_count
from DQUEEN_Result.dbo.dq_result
  where check_id = 77 and stratum1 = 'person') as r4
  on rm1.TABLE_NAME = r4.tbnm and rm1.col = r4.col
left join
  (select
     stratum1 as tbnm
    ,stratum2 as col
    ,count_val as special_char_count
from DQUEEN_Result.dbo.dq_result
  where check_id = 78 and stratum1 = 'person') as r5
  on rm1.TABLE_NAME = r5.tbnm and rm1.col = r5.col
"
cdmPersonInfo <- DatabaseConnector::dbGetQuery(connection,query)


query <- "select
 vy1.*
,vy2.p_10
,vy2.p_25
,vy2.median
from
(select
   stratum1 as table_name
  ,stratum2 as visit_year
  ,count_val
      from DQUEEN_Result.dbo.dq_result
   where check_id = 72
          and stratum1 = 'visit_detail'
            and stratum4 = 'none' ) as vy1
left join
 (select stratum1, stratum2, p_10,p_25, median from DQUEEN_Result.dbo.dq_result_statics where check_id = 72 and stratum1 = 'visit_detail') as vy2
  on vy1.visit_year = vy2.stratum2"
cdmvisitValidation2 <- DatabaseConnector::dbGetQuery(connection,query)
cdmvisitValidation2$visit_year <- as.numeric(cdmvisitValidation2$visit_year)
cdmvisitValidation2$count_val <- as.numeric(cdmvisitValidation2$count_val)
cdmvisitValidation2$p_10 <- as.numeric(cdmvisitValidation2$p_10)
cdmvisitValidation2$p_25 <- as.numeric(cdmvisitValidation2$p_25)
cdmvisitValidation2$median <- as.numeric(cdmvisitValidation2$median)



query <- "select * from DQUEEN_Result.dbo.data_19"
visitValidation <- DatabaseConnector::dbGetQuery(connection,query)

query <- "select
       stratum1 as tbnm
      ,case
       when stratum2 = 9203 then 'Emergency Room Visit'
       when stratum2 = 9202 then 'Outpatient Visit'
       ELSE 'Inpatient Visit'
       END as patfg
      ,stratum3 as visit_year
      ,count_val
      ,P_25
  from DQUEEN_Result.dbo.dq_result_statics where check_id = 73 and stratum1 = 'visit_detail'"
cdmVisitData <- DatabaseConnector::dbGetQuery(connection,query)




query <- "select * from DQUEEN_Result.dbo.data_20"
cdmVisitLengthStatic <- DatabaseConnector::dbGetQuery(connection,query)

query<-'select * from DQUEEN_Result.dbo.data_21 order by patfg;'
cdmVisitLength <- DatabaseConnector::dbGetQuery(connection,query)


query<- "select * from DQUEEN_Result.dbo.data_22"
cdmDaliyVisitInfo <- DatabaseConnector::dbGetQuery(connection,query)


query<- "select * from DQUEEN_Result.dbo.data_23 order by patfg, daily_visit_cnt"
cdmDaliyVisitType <- DatabaseConnector::dbGetQuery(connection,query)


query<- "select rm1.*,r2.col_count,r3.uniq_count,r4.missing_count,r5.special_char_count from
 (select
       m1.TABLE_NAME
      ,r1.col
      ,m1.rows
      ,r1.dist_count
  from (select TABLE_NAME, rows from DQUEEN_Result.dbo.schema_capacity
    where stage_gb = 'C' and TABLE_NAME = 'visit_detail') as m1
 inner join
  (select
     stratum1 as tbnm
    ,stratum2 as col
    ,count_val as dist_count
from DQUEEN_Result.dbo.dq_result
  where check_id = 74 and stratum1 = 'visit_detail') as r1
  on m1.TABLE_NAME = r1.tbnm) as rm1
 left join
  (select
     stratum1 as tbnm
    ,stratum2 as col
    ,count_val as col_count
from DQUEEN_Result.dbo.dq_result
  where check_id = 75 and stratum1 = 'visit_detail') as r2
  on rm1.TABLE_NAME = r2.tbnm and rm1.col = r2.col
left join
  (select
     stratum1 as tbnm
    ,stratum2 as col
    ,count_val as uniq_count
from DQUEEN_Result.dbo.dq_result
  where check_id = 76 and stratum1 = 'visit_detail') as r3
  on rm1.TABLE_NAME = r3.tbnm and rm1.col = r3.col
left join
  (select
     stratum1 as tbnm
    ,stratum2 as col
    ,count_val as missing_count
from DQUEEN_Result.dbo.dq_result
  where check_id = 77 and stratum1 = 'visit_detail') as r4
  on rm1.TABLE_NAME = r4.tbnm and rm1.col = r4.col
left join
  (select
     stratum1 as tbnm
    ,stratum2 as col
    ,count_val as special_char_count
from DQUEEN_Result.dbo.dq_result
  where check_id = 78 and stratum1 = 'visit_detail') as r5
  on rm1.TABLE_NAME = r5.tbnm and rm1.col = r5.col"
cdmVisitInfo <- DatabaseConnector::dbGetQuery(connection,query)

DatabaseConnector::disconnect(connection = connection)
rm(connection)
rm(ConnectionDetails)
rm(threshold)

#Save RDS (if you have your DB Result)

resultPath <- file.path(getwd(),'data')
for(i in 1:length(ls())){
  saveRDS(get(ls()[i]),paste0(resultPath,'/',ls()[i],'.RDS'))
}
