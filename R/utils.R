# Hail's基础性功能函数


#' 获取icloud云盘地址。
#' 需要区分系统：windows or mac
#' @return 返回NULL 或 相应icloud文件夹地址
#' @export
getCloudr <- function(){
  cdr = NULL
  sysn = Sys.info()['sysname']
  if(sysn == 'Windows'){
    cdr = 'C:/Users/Administrator/iCloudDrive'

  }else if(sysn == 'Darwin'){
    #macos
    cdr = '/Users/longsa/Library/Mobile Documents/com~apple~CloudDocs'
  }else{
    stop('get system name error!\n')
  }

  return(cdr)
}





#' 根据项目当前位置获取参数的绝对地址,get absolute address
#' 若地址开头为 * 则获取icloud云盘地址
#' @param adr 文件地址
#' @return 属性credb标识adr是否存在
#' @export
getAbsr <- function(adr){

  res = ''

  #根据/分解路径
  if(!is.character(adr) || adr == '')
    pd = ''
  else{
    pd = strsplit(adr, '\\/')
    pd = unlist(pd)
  }


  #开头为空或为~，为mac或linux完整地址
  if(pd[1] %in% c('', '~')){
    res = adr

    #开头为A-Z字幕；为windows完整地址
  }else if(grepl('[a-zA-Z]\\:', pd[1])){
    res = adr

    #开头为.；为当前位置的相对地址
    #需先获取当前完整地址
  }else if(pd[1] == '.'){
    nd = getwd()
    adr = gsub('^\\.', '', adr)
    res = paste0(nd, adr)

    #开头为..；为当前上级位置的相对地址
  }else if(pd[1] == '..'){
    nd = getwd()
    nd = strsplit(nd, split = '\\/')
    nd = unlist(nd)

    nd = nd[-length(nd)]
    nd = paste(nd, collapse = '/')

    adr = gsub('^\\.\\.', '', adr)
    res = paste0(nd, adr)

    #开头为字幕数字或下划线组合，为相对地址
  }else if(grepl('[a-zA-Z0-9_].*', pd[1])){
    nd = getwd()
    res = paste(nd, adr, sep = '/')

    #若地址开头为*；则为icloud云盘地址
    #需要区分系统：windows or mac
  }else if(pd[1] == '*'){
    nd = getCloudr()
    adr = gsub('^\\*', '', adr)
    res = paste0(nd, adr)
  }

  if(!res == ''){
    #若无文件后缀，则判断为文件夹，最后需加上/
    sfx = strsplit(res, '\\/')
    sfx = unlist(sfx)
    sfx = sfx[[length(sfx)]]
    if(!grepl('\\.', sfx)){
      res = paste0(res, '/')
      res = gsub('\\/{2,}', '/', res)
    }
  }

  #增加格外属性credible
  attr(res, 'credible') <- 0
  if(file.exists(res)) attr(res, 'credible') <- 1

  return(res)
}




#' 检查cols是否是dt的列
#' @param dt 为list,data.table,data.frame等包含列名的数据
#' @param cols 为待验证的列名
ckCols <- function(dt, cols){
  r = F
  if(is.null(dt)) return(r)

  # if(inherits(dt, 'list')) dt = rbindlist(dt, fill = TRUE)

  #cols都为character，且都包含在dt的列名中，则返回T
  if(all(is.character(cols)) && all(cols %in% names(dt))){
    r = T
  }

  return(r)
}





#' 下载文件到指定地址，提供文件链接及对应的文件名
#' @param url 待下载文件的URL地址
#' @param dest 文件下载地址，包含文件名
#' @param dw 存在同名文件是否下载，默认为0不下载
#' @return 下载成功返回地址，下载失败返回0
#' @export
downa <- function(url, dest = './', dw = 0){
  #判断dest为地址还是文件名，若为地址需另配文件名

  dest = getAbsr(dest)
  r = 0
  #dw=1、或文件不存在、或文件存在但大小为0时下载
  if(dw == 1 || !file.exists(dest) || file.size(dest) == 0){
    r = try(download.file(url, dest, method = 'libcurl', mode = 'wb'),
            silent = T)
  }

  if(inherits(r, 'error')){
    dest = ''
  }else if(file.size(dest) == 0){
    file.remove(dest)
    dest = ''
  }

  return(dest)
}




#' 指定小数点位数，并且丢弃指定位数后的数字
#' 对plotly的使用十分重要
#' @param x 待处理的数字
#' @param digit 小数点位数
#' @return 返回double类型数据，小数点位数digit
#' @export
roundt <-  function(x, digit = 2) {
  if (!is.numeric(x)) {
    message('x must be numeric/double.\n')
    return(x)
  }

  d <- 10^digit

  r <- trunc(x * d) / d
  return(r)
}



#' 将dt中指定列的数值转换为指定长度单位
#' @param dt data.table数据
#' @param mcols 为需要进行转换的列
#' @param digit 进行转换的单位基数，默认为1，不进行转换
#' @param skipx 为“名-值”对，表示此列对应值所在行的mcols列的值不进行转换
#' @return 返回转换后的数据
#' @export
setDigit <- function(dt, mcols = 'value', digit = 1, skipx = list()){

  if(!inherits(dt, 'data.table')){
    message('dt must be data.table.\n')
    return(dt)
  }


  if(!ckCols(dt, mcols)){
    message('mcols do not all involved in dt.\n')
    return(dt)
  }


  dw = copy(dt)
  dw = unique(dw)

  #增加辅助列:是否跳过此行，默认为0不跳过
  dw[, skp := 0]


  #存在可变参数，并对相应的列复制跳过转换标识
  if(!inherits(skipx, 'list')){
    message('skipx must be named list. Ignore!\n')

  }else if(length(skipx) > 0){
    #获取可变参数的列名
    xcols = names(skipx)

    if(!ckCols(dt, xcols)){
      message('skipx involved columns which not in dt. Ignore!\n')

      #标识不进行转换的行，将skp更改为1；
    }else{
      for(i in seq_along(skipx)){
        dw[get(xcols[i]) %in% skipx[[i]], skp := 1]
      }
    }
  }

  #标记mcols中包含%的行skp为1
  for(m in mcols)
    dw[grepl('%', get(m)), skp := 1]

  #去掉mcols列的百分号，并转换为double类型
  dw[, c(mcols) := map(.SD, ~gsub('%', '', .x)), .SDcols = mcols]

  #suppressWarnings关闭警告通知
  dw[, c(mcols) := map(.SD, ~roundt(suppressWarnings(as.double(.x)), 2)),
     .SDcols = mcols]

  #对mcols列进行百万单位转换，跳过skp为1的行
  dw = dw[skp == 0,
          c(mcols) := lapply(.SD/digit, roundt, 2),
          .SDcols = mcols]

  dw[, skp := NULL]

  return(dw)
}



#' 根据patterns-reps对进行文本替换，patterns可为数字串
#' 如x为数值型，默认增加|转换为字符，且reps前后增加|的匹配
#' 如|234|23|，替换23时，不替换｜234｜中的23.
#' @param x 待进行文本替换的字符或者数字
#' @param pats 待替换的文本/数字串
#' @param reps 替换后的文本串；与pats一一对应
#' @return 返回替换后的文本串
#' @export
replas <- function(x, pats, reps){

  if(!is.atomic(x)){
    warning('x must be character vector.')
    return(x)
  }

  #x为数值，则转换为字符，并加“|”
  if(is.numeric(x)){
    x = paste0('|', x, '|')
    #pats前后增加|的匹配
    pats = paste0('(?<=\\|)', pats, '(?=\\|)')

  }

  len.p = length(pats)
  len.r = length(reps)

  #pats-reps配对
  if(len.r > len.p){
    #pats少于reps，丢弃多余的reps
    reps = reps[1:len.p]
    message('reps longer than pats；discard surplus reps!\n')

  }else if(len.p > len.r){
    #pats多于reps，则将多的pats以|合并到最后一个pats字符
    rb = c(len.r:len.p)

    pats[len.r] = paste(pats[rb], collapse = '|')
    pats = pats[1:len.r]
    message('pats longer than reps; collapsed pats.\n')

    #去掉重复的｜
    pats = gsub('\\|{2,}', '|', pats)
  }

  #字符替换
  #每个pattern-rep对，都要替换一次
  for(i in seq_along(reps)){
    pt = pats[i]
    rp = reps[i]
    x = gsub(pt, rp, x, perl = T)
  }

  #去掉头尾的|
  x = gsub('^\\||\\|$', '', x, perl = T)

  return(x)
}


#' 对指定字段的数据进行去重操作，deDuplication,deDups
#' @param dt 待去重数据，可为data.frame，data.table以及list类型数据
#' @param rpcols repeat coloumns,为需去重的列,存在多个相同值，可为多列。
#' @param dwcol 去重依据列,可为id、日期时间类，取值唯一。
#' @param ori 为去重方向，1为保留dwcol中的最大值，0为保留dwcol中的最小值
#' @return 返回去重后的data.table数据
#' @description 如果rpcols对应的dwcol包含有效值与NA，NA会全部保留,
#' @description 如果rpcols对应的dwcol全部为NA，不处理(仅做全部列的unique处理)
#' @export
deDups <- function(dt, rpcols = NULL, dwcol = NULL, ori = 1){

  if(is.null(dt)) {
    message('dt is null.\n')
    return(dt)
  }
  dw = copy(unique(dt))


  if(inherits(dw, 'list')) dw = rbindlist(dw)
  dw = setDT(dw)[]
  dw = unique(dw)

  #根据ori的值，判断使用max或min函数
  if(ori == 1){
    mix = max
  }else if(ori == 0){
    mix = min
  }else{
    stop('ori error! only:1/0')
  }

  #判断列参是否包含在dt中
  if(!ckCols(dw, union(rpcols, dwcol))){
    stop(paste0('dt has no col named: ', paste0(rpcols, collapse = '|'),
                ' or ', dwcol))
  }

  #引入辅助列，保留数据顺序
  dw[, ord := seq_along(dwcol)]

  #抽出dwcol为NA的数据
  dw[, xna := all(is.na(get(dwcol))), by = rpcols]
  dwn = dw[xna == T]
  dwa = dw[xna == F]

  dwn = unique(dwn)
  #引入辅助列; get函数获取对应列的值
  dwa[, aux := mix(get(dwcol), na.rm = T), by = rpcols]
  dwa = dwa[aux == get(dwcol)]
  dwa[, aux := NULL]


  dw = rbind(dwa, dwn)
  dw = dw[order(ord)]
  dw[, c('ord', 'xna') := NULL]

  return(dw)
}


#' 固定增长率的复利现值, pmt(payment for the first period)
#' @param r 折现率
#' @param g 增长率
#' @param n 期数
#' @param pmt payment for the first period
#' @return 返回复利现值
#' @export
pv.growth <- function(r, g, n, pmt = 1){

  #先计算增长率下的未来值
  f = FinCal::fv(g, n, pv = pmt)

  #再计算未来值f的现值
  pva = FinCal::pv(r, n, fv = f)
  pva = abs(pva)

  return(pva)
}


#' 固定增长率的年金现值,pmt(payment for the first period)
#' @param r 折现率
#' @param g 增长率
#' @param n 期数
#' @param pmt payment for the first period
#' @return 返回年金现值
#' @export
pv.annu.growth <- function(r, g, n, pmt = 1){

  if(n == 1){
    pva = pv.growth(r = r, g = g, n = n, pmt = pmt)

  }else if(n > 1){
    pva = pv.growth(r = r, g = g, n =n, pmt = pmt) +
      pv.annu.growth(r =r, g = g, n = n-1, pmt = pmt)

  }

  return(pva)
}



#' 基于俩文本Character之间的编辑距离，计算文本相似度
#' @param a 文本a
#' @param b 文本b
#' @param methd 方法，默认lv
#' @return 返回文本相似度数据
#' @export
Csimilar <- function(a, b, methd = 'lv'){

  if(is.character(a) & is.character(b)){

    edst = stringdist::stringdist(a, b, method = methd)

    #没明白！为何分母为a、b之间的最大值
    r = 1 - edst/max(nchar(a), nchar(b))
    r = round(r*100, 2)

  }else{

    r = NA
  }

  return(r)
}


#' 计算俩片段间重叠的时间time长度与第一个片段长度之比
#' @param sega 片段a的开始-结束时间
#' @param segb 片段b的开始-结束时间
#' @param methd 选择比较方式，暂时没有！
#' @return 返回相似度数据
#' @export
Tsimilar <- function(sega, segb, methd = 'x'){

  ad = strsplit(sega, '-')[[1]]
  ad = as.double(ad)
  bd = strsplit(segb, '-')[[1]]
  bd = as.double(bd)

  if(any(is.na(ad), is.na(bd))){
    r = 0
  }

  maxs = max(ad[[1]], bd[[1]])
  mine = min(ad[[2]], bd[[2]])

  if(maxs >= mine){

    r = 0
  }else{

    r = round((mine - maxs)/(ad[[2]] - ad[[1]])*100, 2)
  }

  return(r)
}



#' 根据开始日期，以及给定日期，获取从基准日期开始的第x周/月
#' @param rdate 给定的待获取的基于基准日期开始计算的日期
#' @param sdate 基准开始日期
#' @param width 计算间隔，可以是day/week/month
#' @return 返回是第几day/week/month
#' @export
getDtag <- function(rdate = NULL, sdate = ymd(20250101), width = 'week'){

  if(!(lubridate::is.Date(rdate) | lubridate::is.POSIXct(rdate))){
    return(0)
  }

  if(width == 'week'){

    xwd = weeks(1)
  }else if(width == 'month'){

    xwd = months(1)
  }else if(width == 'day'){

    xwd = days(1)
  }

  r = as.numeric(lubridate::interval(sdate, rdate+1)/xwd)
  r = ceiling(r)

  return(r)

}
