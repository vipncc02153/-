
[exec command :ssh 10.107.217.147 'sh /home/mingyong/MA_OM_VENDOR/MA_OM_VENDOR.sh']
start-----------------------Tue Mar  1 07:50:39 2016--------------------------------

R version 3.1.2 (2014-10-31) -- "Pumpkin Helmet"
Copyright (C) 2014 The R Foundation for Statistical Computing
Platform: x86_64-pc-linux-gnu (64-bit)

R is free software and comes with ABSOLUTELY NO WARRANTY.
You are welcome to redistribute it under certain conditions.
Type 'license()' or 'licence()' for distribution details.

  Natural language support but running in an English locale

R is a collaborative project with many contributors.
Type 'contributors()' for more information and
'citation()' on how to cite R or R packages in publications.

Type 'demo()' for some demos, 'help()' for on-line help, or
'help.start()' for an HTML browser interface to help.
Type 'q()' to quit R.
install.packages("plm")
> source("/home/mingyong/MA_OM_VENDOR/MA_OM_VENDOR_connectOracle.R")
Loading required package: DBI
> library(plyr)
> library(parallel) 
> library(doMC)
Loading required package: foreach
Loading required package: iterators
> library(stringr)
> Sys.setenv(TZ = "G.MT")
> Sys.setenv(ORA_SDTZ = "GMT")
> registerDoMC(cores=30) #并行化，并行核数
> 
> #获取原始数据
> get_data <- function(){
+   con <- dbConnect144()
+   query <-"select * from MA_OM_DETAIL where cm_name != 'HW' and last_test_flag=1 and ((operation_sequence ='3' and operation_sequence_sub = '1') or (operation_sequence ='0' and operation_sequence_sub = '1')) order by cm_name,vendor,bom,module,sub_module,r2_start_datetime"
+   res <- dbGetQuery(con,query)
+   dbDisconnect(con)
+   return(res)
+ }
> 
> #获取各维度下计算标注差所需的均值
> get_mean <- function(){
+   con <- dbConnect144()
+   query <-"select * from PM_MA_VENDOR_MEAN_STD"
+   res <- dbGetQuery(con,query)
+   dbDisconnect(con)
+   return(res)
+ }
> 
> 
> #获取各维度下的STD1、STD2
> get_std<-function(){
+   con <- dbConnect144()
+   query <-"select * from MA_OM_VENDOR_SIGMA"
+   res <- dbGetQuery(con,query)
+   dbDisconnect(con)
+   return(res)
+ }
> 
> #获取各bom下的上下限
> get_limit<-function(){
+   con<-dbConnect144()
+   query <- "select * from OM_LIMIT_CONFIG"
+   res <- dbGetQuery(con,query)
+   dbDisconnect(con)
+   return(res)
+ }
> 
> output_pm_ma_data<-function(pm_ma_data){
+   con <- dbConnect144()
+   query<- "insert into MA_OM_VENDOR (VENDOR,BOM,TIME,UPPER_BOUND,LOWER_BOUND,MEAN_25,MEAN_10,SD_GRP,MODULE,SUB_MODULE,START_TIME,END_TIME,CM_NAME) values
+   (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)"
+   #dbGetQuery(con,"delete from PM_MA_TEST")
+   rs<- dbSendQuery(con,query, data = pm_ma_data)
+   dbDisconnect(con)
+ }
> 
> output_ma_om_alerts_data<-function(ma_om_alerts_data){
+   con<-dbConnect144()
+   query<- "insert into MA_OM_VENDOR_ALERT (ALERT_ID,VENDOR,BOM,JOB_NUM,MODULE,SUB_MODULE,CM_NAME,ALERT_DATETIME,FROM_DATETIME,TO_DATETIME
+   ,LOWER_LIMIT,UPPER_LIMIT,DIM_NUM,ALERT_DESC,ALERT_LEVEL,BATCH_VOLUME,ALERT_PIC) values (sys_guid(),:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16)"
+   #dbGetQuery(con,"delete from MA_OM_ALERTS_TEST")
+   rs<- dbSendQuery(con,query, data = ma_om_alerts_data)
+   dbDisconnect(con)
+ }
> 
> #按'VENDOR','BOM','MODULE','SUB_MODULE','CM_NAME'获取正常数据频率分布曲线
> get_density_data <- function(){
+   con<-dbConnect144()
+   query <- "SELECT * FROM MA_OM_DENSITY_DATA"
+   res <- dbGetQuery(con,query)
+   dbDisconnect(con)
+   return(res)
+ }
> #问题数据的频率分布直方图,结合同纬度下正常数据的频率分布曲线（pic改进）
> create_pic_hist<-function(data){
+   normal_data <- get_density_data()
+   pic_name_list <- c()
+   for (i in 1:dim(data)[1]){
+     data_one <-data[i,c('VENDOR','BOM','MODULE','SUB_MODULE','CM_NAME')]
+     temp_data<-merge(data_one,pm_om_data,by=c('VENDOR','BOM','MODULE','SUB_MODULE','CM_NAME'))
+     start_time_t<-data[i,c('START_TIME.x')]
+     end_time_t<-data[i,c('END_TIME.x')]
+     temp_data<-temp_data[temp_data$R2_START_DATETIME>=start_time_t&temp_data$R2_START_DATETIME<=end_time_t,]
+     cm_name<-data[i,'CM_NAME']
+     board_code<-data[i,'BOM']
+     num_time <- as.integer(as.POSIXct(Sys.time()))  
+     file_name = paste(sep="_","urgent",cm_name,board_code,i,num_time) 
+     pic_dst = paste(sep= "","/home/jdm/image/",file_name,".png")
+     png(filename = pic_dst)
+     t<-hist(temp_data[temp_data$R3_RESULT_DESC<9,]$R3_RESULT_DESC,breaks=seq(1,9,0.2))  
+     tt<-normal_data[normal_data$VENDOR==data_one$VENDOR&normal_data$BOM==data_one$BOM&normal_data$MODULE==data_one$MODULE&normal_data$SUB_MODULE==data_one$SUB_MODULE&normal_data$CM_NAME==data_one$CM_NAME,6:45]
+     tt<-round(tt,8)#控制数字精度
+     #t$mids为x,t$counts/sum(t$counts)为y1,normal_data维度下的数据为y2
+     plot_str<-"["
+     for(n in 1:dim(tt)[2]){
+       str1<-paste("{x:",t$mids[n],",y1:",round((t$counts/sum(t$counts))[n],8),",y2:",tt[n],"}",seq="")
+       #str1<-paste(seq="-",t$mids[n],(t$counts/sum(t$counts))[n],tt[n])
+       if(n==1){
+         plot_str<-paste(plot_str,str1,seq="")
+       }else{
+         plot_str<-paste(plot_str,",",str1,seq="")
+       }
+     }
+     plot_str<-paste(plot_str,"]",seq="")
+     pic_name_list[i]<-plot_str
+     dev.off()  
+   }
+   return (pic_name_list)
+ }
> 
> 
> 
> #同纬度下的数据打批编号
> SplitByBatch<-function(x,n){
+   x<-x[order(x$R2_START_DATETIME),]
+   l<-dim(x)[1]
+   ls<-1:l
+   lsb<-floor((ls-1)/n)+1
+   x$gid<-lsb
+   x
+ }
> 
> #移动平均计算
> m_mean<-function(x,start_gid,stop_gid,m_window,c_col){
+   m_func(x,start_gid,stop_gid,m_window,c_col,"mean")
+ }
> 
> #移动标准差计算
> m_sd<-function(x,start_gid,stop_gid,m_window,c_col){
+   m_func(x,start_gid,stop_gid,m_window,c_col,"sd")
+ }
> 
> #自定义方差计算函数
> new_sd_func<-function(x,mean_value){
+   sd_value=0
+   for(i in 1:length(x)){
+     sd_value=sd_value+(x[i]-mean_value)*(x[i]-mean_value)
+   }
+   return(sqrt(sd_value/length(x)))
+ }
> 
> #自定义移动方差计算函数
> sd_grp_func<-function(x,start_gid,stop_gid,m_window,c_col){
+   registerDoMC(cores=30)
+   vendor=x[1,'VENDOR']
+   cm_name=x[1,'CM_NAME']
+   module=x[1,'MODULE']
+   sub_module=x[1,'SUB_MODULE']
+   bom=x[1,'BOM']
+   mean_pm_30<-subset(mean_data,mean_data$VENDOR==vendor&mean_data$CM_NAME==cm_name&mean_data$MODULE==module&mean_data$SUB_MODULE==sub_module&mean_data$BOM==bom)
+   registerDoMC(cores=30)
+   aaply(start_gid:stop_gid,.margins=1,function(start) {
+     
+     value<- max(subset(x, x$gid %in% start:(start+m_window-1))[,7])
+     temp_mean_value<-sum(mean_pm_30[,'MEAN'])/dim(mean_pm_30)[1]
+     #print(temp_mean_value)
+     args <- list(x=subset(x, x$gid %in% start:(start+m_window-1))[,c_col],mean_value=temp_mean_value)
+     do.call(new_sd_func, args)
+   }
+   ,.parallel = TRUE
+   )
+ }
> #c_col是r3_result_desc,do.call是针对args用func()进行计算
> m_func<-function(x,start_gid,stop_gid,m_window,c_col,func){
+   registerDoMC(cores=30)
+   aaply(start_gid:stop_gid,.margins=1,function(start) {
+     args <- list(x=subset(x, x$gid %in% start:(start+m_window-1))[,c_col])
+     do.call(func, args)
+   }
+   ,.parallel = TRUE
+   )
+ }
> 
> 
> #以批为单位计算移动计算均值
> ma<-function(x,grp_dim,m_window_mid,m_window_short,c_col){
+   registerDoMC(cores=30) #并行化，并行核数
+   #分组内的起止时间统计
+   ns<-append(grp_dim,"gid")
+   rs<-ddply(x,ns,
+             function(ttt){
+               #ttt<-droplevels(ttt)
+               data.frame(min(ttt$R2_START_DATETIME),max(ttt$R2_START_DATETIME))
+             }
+             ,.parallel = TRUE #并行化
+   )
+   
+   #计算中期均值和标准差
+   g_start<-1
+   g_stop<-max(x$gid)
+   mean_grp<-m_mean(x,g_start,g_stop,1,c_col)#组内均值
+   rs$mean_grp<-mean_grp
+   g_stop<-max(x$gid)-m_window_mid+1
+   mean_mid<-m_mean(x,g_start,g_stop,m_window_mid,c_col)#中期均值
+   sd_mid<-m_sd(rs,g_start,g_stop,m_window_mid,"mean_grp")#中期标准差
+   
+   #计算短期均值
+   g_stop<-max(x$gid)-m_window_short+1
+   mean_short<-m_mean(x,g_start,g_stop,m_window_short,c_col)#短期均值
+   sd_short<-sd_grp_func(x,g_start,g_stop,m_window_short,c_col)#移动批次标准差
+   rs<-rs[(m_window_mid):(dim(rs)[1]),]#去除前面为有中期值的部分
+   
+   rs$mean_mid<-mean_mid
+   rs$sd_mid<-sd_mid
+   
+   mean_short<-mean_short[(m_window_mid-m_window_short+1):(length(mean_short))]#去除有短期值，但未有中期值的部分
+   sd_short<-sd_short[(m_window_mid-m_window_short+1):(length(sd_short))]#去除有短期值，但未有中期值的部分
+   job_n <- x[1:(length(sd_short)),"JOB_NUM"]
+   #   print(job_n)
+   rs$job_num <- job_n
+   rs$mean_short<-mean_short
+   rs$sd_grp<-sd_short
+   rs
+ }
> 
> #移动平均线，x为处理的数据，BATCh_volume为批内样本量，比如30，moving_window_mid为中期计算窗口，比如25，
> Mmeansd<-function(x,grp_dim,batch_volume,moving_window_short,moving_window_mid,c_col){
+   x2<-SplitByBatch(x,batch_volume)
+   if((dim(x2)[1])>=(batch_volume*moving_window_mid))
+     ma(x2,grp_dim,moving_window_mid,moving_window_short,c_col)
+ }
> 
> co_evl<-function(a,grp_dim,batch_volume,window_short,window_mid,c_col){
+   registerDoMC(cores=30) #并行化，并行核数
+   at<-ddply(a,grp_dim,function(aaa){Mmeansd(aaa,grp_dim,batch_volume,window_short,window_mid,c_col)}
+             ,.parallel = TRUE # 并行化
+   )
+   at$upper<-at$mean_mid+at$sd_mid#上包络线
+   at$lower<-at$mean_mid-0.9*at$sd_mid#下包络线
+   at
+ }
> 
> #------------------------以下是增量计算-------------------------------
> 
> 
> #------------------------以下是主函数---------------------------
> 
> mean_data<-get_mean()
> pm_om_data<-get_data()
> pm_om_data<-pm_om_data[pm_om_data$R3_RESULT_DESC>0,]
> ttt<-co_evl(pm_om_data,c("VENDOR","BOM","MODULE","SUB_MODULE","CM_NAME"),30,10,25,"R3_RESULT_DESC")
> 
> pm_ma<-ttt[,c("VENDOR", "BOM", "min.ttt.R2_START_DATETIME.","upper","lower","mean_mid","job_num","mean_short","sd_grp","MODULE","SUB_MODULE","min.ttt.R2_START_DATETIME.","max.ttt.R2_START_DATETIME.","CM_NAME","gid")]
> names(pm_ma)<-c("VENDOR", "BOM","TIME","UPPER_BOUND","LOWER_BOUND","MEAN_25","JOB_NUM","MEAN_10","SD_GRP","MODULE","SUB_MODULE","START_TIME","END_TIME","CM_NAME","GID")
> pm_ma_data<-cbind(pm_ma[c("VENDOR","BOM", "TIME","UPPER_BOUND","LOWER_BOUND","MEAN_25","MEAN_10","SD_GRP","MODULE","SUB_MODULE","START_TIME","END_TIME","CM_NAME")])
> 
> #预警列表
> std_data <-get_std()
> # 筛选出穿出下包络线(MEAN_10<LOWER_BOUND) 并且10标准差大于STD2的数据点
> outer_lower_bound_data <-pm_ma[pm_ma$MEAN_10<pm_ma$LOWER_BOUND,]
> merge_data <-merge(outer_lower_bound_data,std_data,by=c("VENDOR","BOM","MODULE","SUB_MODULE","CM_NAME"))
> merge_data_2 <-merge_data[merge_data$TIME>merge_data$START_TIME.y & merge_data$TIME<merge_data$END_TIME.y,]
> warn_data_1 <-na.omit(merge_data_2[merge_data_2$SD_GRP>merge_data_2$STD2,]) #注意此处的merge_data_2$STD表示2被的标准差
> warn_data_1 <- warn_data_1[warn_data_1$SD_GRP>warn_data_1$STD2,]
> 
> warn_data_2 <- warn_data_1[,c("VENDOR","BOM","JOB_NUM","MODULE","SUB_MODULE","CM_NAME","TIME","START_TIME.x","END_TIME.x","GID")]
> warn_data_3 <-warn_data_2[order(warn_data_2$TIME),]
> 
> if(dim(warn_data_3)[1]>=1){
+   #提取TIME中的年月日作为date_str维度，然后对"VENDOR","BOM","MODULE","SUB_MODULE","CM_NAME","date_str"分割滤掉某天预警点多余1条的数据
+   require(stringr)
+   limit_data <-get_limit()
+   warn_data<-ddply(warn_data_3,c("VENDOR","BOM","MODULE","SUB_MODULE","CM_NAME"),function(x){
+     #print(x)
+     id=c()
+     num=1
+     if(dim(x)[1]==1){
+       id[1]=1
+     } else if (dim(x)[1]==2){
+       if((x[1,10]+1)==x[2,10]){
+         id[1]=1
+         id[2]=1
+       } else {
+         id[1]=1
+         id[2]=2
+       }
+     } else {
+       for(i in 1:(dim(x)[1]-1)){
+         if((x[i,10]+1)==x[i+1,10]){
+           id[i]<-num
+           
+         } else {
+           id[i]<-num
+           num<-num+1
+         }
+         if((i==(dim(x)[1]-1))&(i!=1)){
+           id[i+1]<-num
+         }
+       }
+       
+     }
+     #print(id)
+     limit<-subset(limit_data,limit_data$BOM==x[1,2]&limit_data$MODULE==x[1,4])[,3:4]
+     x<-cbind(x,limit,id)
+     }
+   
+   )
+   
+   warn_data_per_day_one <-ddply(warn_data,c("VENDOR","BOM","MODULE","SUB_MODULE","CM_NAME","id"),function(x){
+     
+     x[1,8]<-min(x[,8])
+     x[1,9]<-max(x[,9])
+     dim<-dim(x)[1]
+     
+     if(dim(x)[1]>2)#连续三个或以上为严重
+     {
+       des<- paste("发送光功率在",substr(x[1,7],1,10),"，连续",dim,"点超过预警点",seq="")
+       level<-"严重"
+       x<-cbind(x[-10],des,level)
+     }
+     else if(dim(x)[1]==2)
+     { des<- paste("发送光功率在",substr(x[1,7],1,10),"，连续",dim,"点超过预警点",seq="")
+       level<-"一般"
+       x<-cbind(x[-10],des,level)
+     }
+     else
+     {
+       des<- paste("发送光功率在",substr(x[1,7],1,10),"，连续",dim,"点超过预警点",seq="")
+       level<-"提示"
+       x<-cbind(x[-10],des,level)
+     }
+     x$id=dim
+     x<-cbind(x,30)
+     return (x[1,])})
+   
+   pic_name_list <-create_pic_hist(warn_data_per_day_one)
+   
+   ma_om_alerts_data <- cbind(warn_data_per_day_one,pic_name_list)
+   
+   
+   #print("do")
+ }
Warning messages:
1: In data.frame(..., check.names = FALSE) :
  row names were found from a short variable and have been discarded
2: In data.frame(..., check.names = FALSE) :
  row names were found from a short variable and have been discarded
3: In data.frame(..., check.names = FALSE) :
  row names were found from a short variable and have been discarded
> #---------------------------------------------------------------------------------
> #output_pm_ma_data(pm_ma_data)
> #output_ma_om_alerts_data(ma_om_alerts_data)
> get_alert_data <- function(){
+   con <- dbConnect144()
+   query <-"select VENDOR,BOM,MODULE,SUB_MODULE,CM_NAME,max(ALERT_DATETIME) from MA_OM_VENDOR_ALERT group by  VENDOR,BOM,MODULE,SUB_MODULE,CM_NAME"
+   res <- dbGetQuery(con,query)
+   dbDisconnect(con)
+   return(res)
+ }
> get_vendor_data <- function(){
+   con <- dbConnect144()
+   query <-"select VENDOR,BOM,MODULE,SUB_MODULE,CM_NAME,max(END_TIME) from MA_OM_VENDOR group by  VENDOR,BOM,MODULE,SUB_MODULE,CM_NAME"
+   res <- dbGetQuery(con,query)
+   dbDisconnect(con)
+   return(res)
+ }
> last_alert_data<-get_alert_data()
> last_vendor_data<-get_vendor_data()
> 
> 
> insert_pm_ma_data<-ddply(pm_ma_data,c("VENDOR","BOM","MODULE","SUB_MODULE","CM_NAME"),function(x){
+   
+   VENDOR<-x[1,1]
+   BOM<-x[1,2]
+   MODULE<-x[1,9]
+   SUB_MODULE<-x[1,10]
+   CM_NAME<-x[1,13]
+   vendor_time<-last_vendor_data[last_vendor_data$VENDOR==VENDOR&last_vendor_data$BOM==BOM&last_vendor_data$MODULE==MODULE&last_vendor_data$SUB_MODULE==SUB_MODULE&last_vendor_data$CM_NAME==CM_NAME,6]
+   
+   if(length(vendor_time) == 0L){
+     insert_data<-x
+     #print("null")
+   }else{
+     insert_data<-x[x$TIME>vendor_time,]
+     #print("ok")
+   }  
+   return(insert_data)
+ }
+ )
> insert_ma_om_alerts_data<-ddply(ma_om_alerts_data,c("VENDOR","BOM","MODULE","SUB_MODULE","CM_NAME"),function(x){
+   VENDOR<-x[1,1]
+   BOM<-x[1,2]
+   MODULE<-x[1,4]
+   SUB_MODULE<-x[1,5]
+   CM_NAME<-x[1,6]
+   vendor_time<-last_alert_data[last_alert_data$VENDOR==VENDOR&last_alert_data$BOM==BOM&last_alert_data$MODULE==MODULE&last_alert_data$SUB_MODULE==SUB_MODULE&last_alert_data$CM_NAME==CM_NAME,6]
+   if(length(vendor_time) == 0L)
+   {
+     insert_data<-x
+   }else{
+     insert_data<-x[x$TIME>vendor_time,]
+   }
+   return(insert_data)
+ })
> 
> if(dim(insert_pm_ma_data)[1]==0){
+   print("no insert")
+ }else{
+   output_pm_ma_data(insert_pm_ma_data)
+ }
[1] TRUE
> if(dim(insert_ma_om_alerts_data)[1]==0){
+   print("no insert")
+ }else{
+   output_ma_om_alerts_data(insert_ma_om_alerts_data)  
+ }
[1] TRUE
> 
end-------------------------Tue Mar  1 08:26:56 2016---------------------------------
