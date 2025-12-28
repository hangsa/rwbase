

#' @title 校验jqka类型
#' @description 支持的类型: rtype, period and drc.
#' @param x 待校验的值
#' @param jktype jqka校验类型
#' @return F or T
#' @details
#' rtype类型包含的值: debt,benefit,cash,main
#' period类型包含的值: year, report, simple
#' drc类型包含的值: w2l, l2w
#' @export
jkCheck <- function(x = NULL, jktype = NULL){
  r = F
  res = ''

  #jktype为长度为1的字符串
  if(is.null(jktype) || is.null(x)){
    message('arg jktype or x is null!')
    return(r)

  }else if(!is.character(jktype) || length(jktype) != 1 || !jktype %in% c('rtype', 'period', 'drc')){
    message('arg jktype must be one onf rtype/period/drc.')
    return(r)

  }else if(jktype == 'rtype'){
    res = c('debt', 'benefit', 'cash', 'main')

  }else if(jktype == 'period'){
    res = c('year', 'report', 'simple')

  }else if(jktype == 'drc'){
    res = c('w2l', 'l2w')
  }

  #x为长度为1的字符串
  if(!is.character(x) || length(x) != 1){
    message('arg x must be character of length 1.')
    return(r)

  }else if(x %in% res){
    r = T
  }

  return(r)
}




#' @title 建立数据库本地数据库通道
#' @param dbname 数据库名称,默认finace
#' @param dbtype 数据库类型，目前支持postgres 与 mysql
#' @importFrom DBI dbConnect
#' @return 返回已建立的数据库通道
#' @export
dbc.local <- function(dbname = 'finace', dbtype = 'postgres'){
  #需要关联不同的数据库，如用于凉菜分析的diet库

  if(dbtype == 'postgres'){
    dbc = RPostgres::Postgres()
    usr = 'postgres'
  }else if(dbtype == 'mysql'){
    usr = 'root'
    dbc = RMySQL::MySQL()
  }

  fcon = NULL
  #链接本地数据库，当前仅支持postgres
  fcon = try(dbConnect(dbc,
                       host = 'localhost',
                       port = 5432,
                       dbname = dbname,
                       user = usr,
                       password = 'Lihanghi@#0325'),
             silent = T)

  if(inherits(fcon, 'try-error')){
    warning(paste0('established channel to local', dbtype,
                   ' database ', dbname, ' failed.\n'))
  }

  return(fcon)
}




#' 获取Teable下jkdic的api地址
#' @param rtype 财报类型，包含debt,benefit,cash,main，用于search
#' @param take api返回数据量，默认500行数据
#' @param skip 指定api跳过的数量，用于分页获取数据
#' @importFrom httr GET add_headers
#' @return  返回可使用的GET类数据
#' @export
api.jkdic <- function(rtype = 'debt', take = 500, skip = 0){
  #需要能获取别的Teable数据库的api，如用于凉菜数据分析的diet库

  if(!jkCheck(rtype, 'rtype')){
    stop('Arg:rtype must be NULL or  one of [debt|benefit|cash|main].\n')
  }

  if(!is.numeric(take) || !is.numeric(skip)){
    stop(' Arg:take/skip must be numeric.\n')
  }


  url = paste0(c('https://app.teable.cn/api/table/tblo6VIvt3QmwKGRasW/record?take='),
               take, '&skip=', skip)

  if(!is.null(rtype)){
    url = paste0(url, '&search=', rtype,
                 '&search=rtype',
                 '&search=true')
  }

  token = 'teable_accYKEWwq5inqTRzXFR_3m8nLIgbdcgnnjOgJ300sD4TBckjmf96Z6rFnxfRI7o='

  r = httr::GET(url = url,
                add_headers(
                  Authorization = paste0('Bearer ', token),
                  accept = 'application/json'
                  ))

  #未检查r的status_code；使用时检查！
  return(r)
}



#' 获取jkdic数据，从Teable通过api获取，或从本地postgreSQL获取
#' @param rtype 财报类型，包含debt,benefit,cash,main 或 NULL-获取全部类型的数据
#' @param src 数据源，0-从Teable获取数据，1-从本地postgres获取数据；NULL表示按0-1的顺序取数取到即止。
#' @return 返回NULL 或者 data.table数据
#' @export
get.jkdic <- function(rtype = 'debt', src = NULL){

  rdics = NULL
  if(!jkCheck(rtype, 'rtype') && !is.null(rtype)){
    stop('Arg:rtype must be NULL or  one of [debt|benefit|cash|main].\n')
  }

  if(!(is.null(src) || (is.numeric(src) && (src == 0 | src == 1)))){
    stop('Arg:src must be NULL or 0 or 1.\n')
  }



  #从Teable获取数据
  if(is.null(src) || src == 0){

    r = api.jkdic(rtype = rtype)

    if(httr::status_code(r) == 200){
      #提取文本内容
      ct = httr::content(r, 'text', encoding = 'UTF-8')

      dt = jsonlite::fromJSON(ct)
      #转换为data.table
      rdics = dt$records$fields %>% as.data.table()
      rdics = data.table::as.data.table(rdics)
      message('get jkdic from teable succeeded.\n')

    }else{

      message(paste0('get Teable data failed. api stats_code:',
                     httr::status_code(r), '\n'))
      rdics = NULL
    }

    #链接本地postgres-finace数据库
    #src=NULL且从Teable未取到数据，或src=1
  }else if((is.null(src) & is.null(rdics)) || src == 1){

    fcon = dbc.local('finace', 'postgres')

    #构建检索sql
    sql = c("select * from jkdic")
    if(!is.null(rtype)){
      sql = paste0(sql, " where rtype = ",
                   "'", rtype, "'")
    }

    #读取字典中的数据SQL
    rdics = try(DBI::dbGetQuery(fcon, sql),
                silent = T)

    rdics = data.table::as.data.table(rdics)

    if(inherits(rdics, 'try-error')){
      stop('get data from postgres local failed.\n')
    }else{
      message('get jkdic from postgres local succeeded.\n')
    }

    #关闭mysql链接
    DBI::dbDisconnect(fcon)
  }

  #初步处理rdics数据
  if(is.null(rdics) | !is.data.frame(rdics) | nrow(rdics) == 0){
    warning('get 0 data!\n')
  }

  return(rdics)
}





#' 将Teable中的jkdidc数据同步到本地的postgres数据库中作为备份
#' @description
#' 使用rtype+serialno实现唯一定位，使用updatime进行对比，Teable较新则进行同步替换
#' @import data.table
#' @return 1同步成功，0同步失败
#' @export
sync.jkdic <- function(){
  r = 0
  #获取Teable中的jkdic数据表
  teabr = get.jkdic(rtype = NULL, src = 0)

  #获取本地postgres中的jkdic数据表
  postr = get.jkdic(rtype = NULL, src = 1)

  dcols = c('id', 'serialno', 'rtype', 'subject', 'subtype', 'parent', 'name', 'tier',
            'discount', 'rcomments', 'updatime', 'nomil')

  teabr = teabr[, .SD, .SDcols = dcols
                ][, updatime := lubridate::ymd_hms(updatime)]

  postr = postr[, .SD, .SDcols = dcols]


  #基于rtype+serialno关联teabr与postr数据，保留teabr中所有的行
  jks = postr[teabr, on = .(rtype, serialno)]

  #找出变动数据teabr$updatime > postr$updatime
  jks.chg = jks[i.updatime > updatime]


  #连接数据库
  fcon = dbc.local(dbname = 'finace')

  #存在更新的数据，写回本地数据库
  if(nrow(postr) == 0){
    #原表为空，整表写回本地数据库
    DBI::dbWriteTable(conn = fcon,
                      name = 'jkdic',
                      value = teabr,
                      append = TRUE,
                      row.names = FALSE)

    message('update all data of table teabr to local postgres jkdic succeeded.\n')
    r = 1
  }
  else if(nrow(jks.chg) > 0){
    #部分数据更新，按条写回本地数据库

    c1 = setdiff(dcols, c('id', 'rtype', 'serialno', 'updatime'))
    c2 = paste0('i.', c1)

    #循环更新数据库
    for(i in jks.chg$i.id){

      chg = jks.chg[i.id == i]

      for(k in seq_along(c1)){

        #判断对应字段值是否有变化
        if(!identical(chg[[c1[k]]], chg[[c2[k]]])){
          #同字段值不相同，有变化更新数据库
          uqry = paste0("update jkdic ",
                        "set ", c1[k], " = '", chg[[c2[[k]]]],
                        "' where rtype = '", chg[['rtype']],
                        "' and serialno = ", chg[['serialno']]
          )
          DBI::dbExecute(fcon, uqry)

          cat(paste0('update postges.id:', chg[['id']],
                     '; teable.id:', i,
                     '; column:', c1[k],
                     ' ,succeeded.\n'))
        }
      }
    }
    r = 1

    #无数据更新
  }else{

    message(paste0('No data updated from Teable to postgres.\n'))
  }



  #找到postr中有但teabr中没有的行，这些行需要从postgres中删掉
  jks.dlt = postr[!teabr, on = .(rtype, serialno)]

  #存在被删除掉数据，需从postgres库中同步删除
  if(nrow(postr) >0){

    #可以根据postr中的id进行库删除
    for(i in jks.dlt$id){
      dqry = paste0("delete from jkdic where id = ", i)
      DBI::dbExecute(fcon, dqry)

      message(paste0('Delete data id:', i, ' succeeded.\n'))
    }
  }

  DBI::dbDisconnect(fcon)
  return(r)
}
























