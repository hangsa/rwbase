
#' 获取同花顺jqka财报的链接地址
#' @param symb 单个股票代码
#' @param rtype 财报类型，包括debt/benefit/cash/main
#' @param period 财报期数，包括year/report/simple三种
#' @return 返回同花顺财报文件的链接地址
#' @export
jkUrl <- function(symb, rtype = 'debt', period = 'year'){
  url = ''

  if(!jkCheck(rtype, 'rtype') || !jkCheck(period, 'period')){
    warning('rtype or period wrong.')

  }else{
    uri = 'http://basic.10jqka.com.cn/api/stock/export.php?export='
    url = paste0(uri, rtype, '&type=', period, '&code=', symb)
  }

  return(url)
}


#' 链接mysql获取jkdic表数据，扩展jqka数据得到自定义字段数据
#' @description
#' jkdic数据来源:默认Teable，若失败则读本地postgres
#' @param dt 同花顺jqka数据表data.table
#' @param rtype 同花顺数据类型
#' @param corlby 链接字段，需要为dt与jkdic共同名称的字段
#' @importFrom data.table copy setcolorder
#' @return 返回关联了jkdic后的数据data.table
jkCorl <- function(dt, rtype = 'debt', corlby = 'subject'){

  if(!inherits(dt, 'data.table'))
    stop('dt must be data.table.\n')

  if(!ckCols(dt, corlby)){
    stop('corlby must be column of dt.\n')
  }

  dw = copy(dt)
  dw = unique(dw)
  res = NULL

  #读取jkdic数据
  dics = get.jkdic(rtype = rtype)
  if(is.null(dics)){
    stop('get jkdic data failed. Teable and Postgres.\n')
  }

  dcols = c('serialno', 'rtype', 'subject', 'subtype', 'parent', 'name')
  dics = dics[, .SD, .SDcols = dcols]

  if(!ckCols(dics, corlby)){
    stop('corlby must be column of dics.\n')
  }
  #关联jkdic，dw关联jkdic以获取serialno,subtype,parent,name等列信息
  res = merge(dw, dics, by = corlby, all = TRUE)


  if(corlby == 'subject'){
    #得到无name的subject,也就是jkdic中没有配置的subject，并以warning提示
    abn = res[is.na(name)]$subject
    abn = paste(abn, collapse = '|')

    if(abn != '')
      warning(paste0('subject below not in jkdic: ', abn, '\n'))

  }else if(corlby == 'name'){
    #
    res = res[!is.na(symbol)]
  }

  setcolorder(res,
              c('rtype', 'subtype', 'parent', 'serialno', 'name', 'subject'))

  #去掉subtype为discard的数据,保留NA
  #res = res[subtype != 'discard'][order(serialno)]

  return(res)
}



#' 读取同花顺财报文件、简单清洗并关联数据库中的dics
#' @param jkdir 为文件地址(包含文件名)
#' @param rtype jqka财报数据类型:debt,benefit,cash,main
#' @return 返回读取并简单清理后的财报数据data.table
#' @export
jkRead <- function(jkdir = '.', rtype = 'debt'){

  adr = getAbsr(jkdir)

  #文件不存在或文件大小为0，返回NULL
  if(!file.exists(adr) || file.size(adr) == 0){
    warning('file not exists or file size is 0.\n')
    return(NULL)
  }

  #读取文件
  res = try(readxl::read_excel(adr), silent = T)

  if(inherits(res, 'try-error'))
    stop('jkRead read file failed.\n')

  res = data.table::as.data.table(res)

  #简单清洗数据
  names(res)[1] = 'subject'

  #清理原始财报数据中各项名称
  #括号中的所有文本转为空；顿号、转为斜杠/；去掉顿号前的一二三、123
  pats = c('[(（].*?[)）]|[:：]', '[一二三四五六七八九十123456789]\\、', '\\、')
  #上面清除括号内内容，使用正则表达式的非贪婪模式
  reps = c('', '', '/')
  res[, subject := replas(subject, pats, reps)]

  #去掉其中包含 “*|指标” 字符的行
  res = res[!grepl('\\*|指标', subject)]

  #关联mysql-jkdic
  res = jkCorl(res, rtype = rtype, corlby = 'subject')

  return(res)
}



#' 转换同花顺财报数据，宽数据<->长数据
#' @param dt data.table格式数据
#' @param drc 转换方向, wide to long(w2l), long to wide(l2w)
#' @importFrom data.table melt dcast setcolorder patterns
#' @return 返回转换后的数据
#' @export
jkConvert <- function(dt, drc = 'w2l' ){

  if(!inherits(dt, 'data.table')){
    stop('dt must be data.table')
  }

  #宽数据转长数据
  if(drc == 'w2l'){
    #判断是否有年份类的列，没有返回错误
    if(!any(grepl('[0-9]{4}', names(dt), perl = T))){
      stop('dt has no datelike column.')

    }

    #转换为长数据，将年份列转为单一列，注意patterns的应用！！！
    res = melt(dt,
               measure.vars = patterns('[0-9]{4}.*'),
               variable.name = 'date',
               value.name = 'value',
               variable.factor = FALSE)

    #处理年份字段date
    if(grepl('^[0-9]{4}$', res$date[1])){
      res[, date := as.numeric(date)]
    }else{
      res[, date := lubridate::ymd(date)]
    }


    res = res[order(-date, serialno)]

    #长数据转宽数据
  }else if(drc == 'l2w'){
    #得到包含‘date’的列
    dcols = names(dt)
    dc = dcols[grepl('date', dcols)]

    if(length(dc) == 1){
      #将date列转换为多个年份列
      formulas = paste0('... ~ ', dc)

      res = dcast(dt, formulas, value.var = 'value')

      rcols = names(res)
      #调整年份列的顺序，最新年份要列在最前面
      colDate = rcols[grepl('[0-9]{4,}', rcols)]
      colNdate = rcols[!rcols %in% colDate]

      res = setcolorder(res,
                        c(colNdate, colDate[order(colDate, decreasing = T)]))
      res = res[order(serialno)]

    }else{
      warning('no col date or has multiple col date.\n')
    }
  }

  return(res)
}


#' 将指定列转换为数值，并转换为百万单位
#' @description
#' 若包含百分号%，则不转换。
#' @param dt data.table格式数据
#' @param rtype jqka财报数据类型：debt,benefit,cash,main
#' @importFrom data.table copy is.data.table
#' @return 返回转换后的数据
#' @export
jkMillis <- function(dt, rtype = 'debt'){
  if(!inherits(dt, 'data.table'))
    stop('dt must be data.table')

  dw = copy(dt)
  dw = unique(dw)

  #获取noMilli数据
  nom = get.jkdic(rtype = rtype)

  if(!is.null(nom) & is.data.table(nom) & nrow(nom) > 0){
    nomi = nom[nomil == 1]
    nomi = unique(nomi)
    #单位转换为百万，并跳过skipx中列对应的行
    dw = setDigit(dw, mcols = 'value', digit = 1000000,
                  skipx = list(name = nomi$name))

  }else{
    warning('get.jkdic failed; do not get noMillis.')
    #单位转换为百万
    dw = setDigit(dw, mcols = 'value', digit = 1000000)
  }

  return(dw)
}



#' 计算各财报中需要增加的额外数据及估值
#' @param dt data.table格式数据
#' @param rtype jqka财报类型：debt,benefit,cash,main
#' @param grate 收益增长率，默认0.1增长五年，后续按0.03增长
#' @param xrate 10年期国债收益率,默认0.08
#' @import data.table
#' @importFrom magrittr %>%
#' @return 返回处理后的数据
#' @export
jkCalc <- function(dt, rtype = 'debt', grate = 1, xrate = 0.08){

  if(!inherits(dt, 'data.table'))
    stop('dt must be data.table\n')
  if(!jkCheck(rtype, 'rtype'))
    stop('get rtype wrong.\n')

  if(!all(c('symbol', 'date', 'name', 'value') %in% names(dt)))
    stop('dt has no cols of symbol,date,name or value.\n')

  #转换为以name中的值为主要列的宽数据
  r = dcast(dt, symbol + date ~ name, value.var = 'value')

  #这种方式有个问题，没有判断用于计算的列是否存在
  if(rtype == 'debt'){
    #资产负债表 bookVal, brokeVal未计算
    r[, c('operAst', 'LAR') := .(cast - cliab, round(liab*100/ast, 2))]

  }else if(rtype == 'benefit'){
    #利润表
    r[, c('grossRate','exp3Rate','rschRate','primeRate','profitRate') :=
        .(round((operReven-operCost)*100/operReven, 2), round((salesExp+adminExp+finaceExp)*100/reven, 2),
          round(rschExp*100/reven, 2), round(operProfit*100/profit, 2), round(netProf4u*100/reven, 2))]

  }else if(rtype == 'cash'){
    #现金流量表


  }else if(rtype == 'main'){
    #main主表
    #04.22，需要增加每股有形资产价值计算
    #04.22，需要展示近3年平均每股收益数据

    #计算近五年的平均基本每股收益，平均每股净资产
    r[, c('epsAvg') :=
        .(rowMeans(as.data.table(shift(epsBase, 0:4, type = 'lag')), na.rm = T) %>% round(., 3)),
      by = symbol]

    #简易估值计算,按10年期国债收益xrate（即1/xrate倍pe）的6折
    #这其中潜藏的逻辑是：国债收益率越低，股市估值会越高
    # r[, c('ValPS') := round(epsAvg*0.6/xrate + bvpsAvg*0.7)]

    #按未来现金流折现算法计算
    if(grate == 1){
      #grate为1， 则采用历史平均增长率数据统计,这里采用营业收入的同比增长率
      # r[, c('netr5', 'netr10') :=
      #     .(rowMeans(as.data.table(shift(revenRor, 0:4, type = 'lag')), na.rm = T)/100,
      #       rowMeans(as.data.table(shift(revenRor, 0:9, type = 'lag')), na.rm = T)/100),
      #   by = symbol] #这是求近几年营业增长率的平均值

      r[, c('ror1') := .((reven/shift(reven, 3, type = 'lag'))^(1/3) - 1),
        by = symbol] #求近3年营业收入的年均复合增长率

      #以近3年复合增长率 计算3年后的收入
      #再以折半的增长率，计算10年后的收入
      r[, c('bs1') := FinCal::fv(r = ror1, n = 3, pv = epsAvg) %>% abs()]
      r[, c('bs2') := FinCal::fv(r = ror1/2, n = 7, pv = bs1) %>% abs()]


      r[, c('pval1') := pv.annu.growth(xrate, ror1, 3, epsAvg)] #近3年现金流折现
      r[, c('pval2') := pv.annu.growth(xrate, ror1/2, 7, bs1)] #随后7年现金流折现
      r[, c('pval3') := pv.annu.growth(xrate, 0.03, 10, bs2)] # 载随后十年的现金流折现

      r = r[, !c('ror1')]

    }else{
      # 以预测的增长率grate计算，前五年为grate，5年后增长率减半，10年后增长率固定为3%（通胀率）
      # 关键还是预测的增长率情况！！！
      r[, c('bs1') := FinCal::fv(r = grate, n = 5, pv = epsAvg) %>% abs()] #五年后的收益值
      r[, c('bs2') := FinCal::fv(r = grate/2, n = 5, pv = bs1) %>% abs()] #十年后的收益值

      #以年金现值算法，计算每年收益现金流的折现
      r[, c('pval1') := pv.annu.growth(xrate, grate, 5, epsAvg)]
      r[, c('pval2') := pv.annu.growth(xrate, grate/2, 5, bs1)]
      r[, c('pval3') := pv.annu.growth(xrate, 0.03, 10, bs2)]
    }

    #折现的现金流 + 净资产的七折 为企业估值
    r[, c('ValPS') := round(pval1 + pval2 + pval3)]
    r = r[, !c('bs1', 'bs2', 'pval1', 'pval2', 'pval3')]

  }

  #去掉所有无值（为NA）的name列
  nac = which(colSums(is.na(r)) == nrow(r))
  nna = setdiff(names(r), names(r)[nac])
  r = r[, .SD, .SDcols = nna]

  #将name各列转换为长数据
  r = melt(r, id.vars = c('symbol', 'date'), variable.name = 'name', value.name = 'value')

  #再次关联sql表，用于获取serialno, subject等相关信息
  r = jkCorl(r, rtype = rtype, corlby = 'name')
  #去掉未关联的subject
  r = r[!is.null(subject)]


  return(r)
}


#' 计算指定列wcols的年数值变动率
#' @param dt data.table数据
#' @param wcols forget the usage of this argument
#' @param dcol date column
#' @param vcol value column
#' @import data.table
#' @return 返回计算后的数据表
#' @export
jkYoy <- function(dt, wcols = 'name', dcol = 'date', vcol = 'value'){
  if(is.null(dt)) return(dt)
  dw = copy(dt) %>% unique()


  if(inherits(dw, 'list')) dw = rbindlist(dw)
  dw = setDT(dw)[] %>% unique()

  if(!ckCols(dt, c(dcol, vcol, wcols))){
    stop(paste0('dt has no cols named: ',
                paste0(wcols, collapse = '/'), dcol, ' or ', vcol))
  }


  # dw[, aux := seq_along(get(vcol))]
  dw2 = dw[, .SD, .SDcols = c(wcols, dcol, vcol)]
  setnames(dw2, vcol, 'bval')

  #年份往上+1
  if(lubridate::is.Date(dw2[[dcol]])){
    dw2[, date := date + lubridate::years(1)]

  }else if(is.numeric(dw2[[dcol]])){
    dw2[, date := date + 1]

  }

  res = merge(dw, dw2, by = c(wcols, dcol), all.x = TRUE)

  #计算年度同比
  res[, c('yoy', 'bval') := .(round((get(vcol) - bval)*100/bval, 2), NULL)]

  return(res)
}


#' 下载、读取并清洗计算同花顺jqka财报数据
#' @param symbs 上市公司代码
#' @param rtp jqka财报类型：debt,benefit,cash,main
#' @param prd jqka财报日期：year(年报), report(报告期汇总), simple(单报告期)
#' @param pwd jqka财报文件保存地址
#' @param dw 是否重新下载jqka财报文件
#' @param gr 为公司收益增长率，默认0.15
#' @param xr 为10年期国债收益率,默认0.08
#' @importFrom data.table :=
#' @import magrittr
#' @return 返回财报数据列表list，包括vertical、horizontal两个表
#' @export
jksRep <- function(symbs, rtp = 'debt', prd = 'year', pwd = './', dw = 0, gr = 0.15, xr = 0.08){

  if(!jkCheck(rtp, 'rtype') || !jkCheck(prd, 'period')){
    warning('rtype or period wrong.\n')
    return(NULL)
  }

  #下载同花顺财报文件
  #获取文件链接
  urls = lapply(symbs, jkUrl, rtype = rtp, period = prd)
  names(urls) = symbs

  #构建文件名下载，并返回下载后的文件存放地址
  downs = purrr::map2(urls,
               paste0(pwd, paste(symbs, rtp, prd, sep = '_'), '.xls'),
               downa, dw = dw) %>% unlist()

  #读入所有文件f
  jks = lapply(downs, jkRead, rtype = rtp)

  res = NULL
  #逐一清洗、合并数据
  for(i in seq_along(jks)){
    smb = names(jks)[[i]]

    if(is.null(jks[[i]]))
      next
    else{
      #转变为日期长数据，并将value单位清洗为百万
      dtmp = jkConvert(jks[[i]], 'w2l') %>% jkMillis(., rtype = rtp)
      dtmp[, symbol := smb]
      res = rbind(res, dtmp)
    }
  }

  #添加各财报中需要自己计算的数据
  v = jkCalc(res, rtype = rtp, grate = gr, xrate = xr)
  #计算年度同比增长率
  v = jkYoy(v, wcols = c('rtype', 'subtype', 'parent', 'serialno', 'name', 'symbol', 'subject'),
            dcol = 'date', vcol = 'value')

  v = v[order(-date, serialno)]

  h = jkConvert(v[, !c('yoy')], 'l2w')

  #vertical data for plots.
  res = list(v = v, h = h)
  return(res)
}


