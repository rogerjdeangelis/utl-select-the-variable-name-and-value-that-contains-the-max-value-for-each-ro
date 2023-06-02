%let pgm=utl-select-the-variable-name-and-value-that-contains-the-max-value-for-each-row;

select the variable name and value that contains the max value for each row

github
https://tinyurl.com/47uhymx3
https://github.com/rogerjdeangelis/utl-select-the-variable-name-and-value-that-contains-the-max-value-for-each-ro

StackOverflow; R
https://tinyurl.com/y6affa3h
https://stackoverflow.com/questions/17735859/for-each-row-return-the-column-name-of-the-largest-value

  1. PURE SQL
      0. No sql in SAS and wps
      1. No SQL r
      2. Python just sql
      3. WPS sql array
      4. R just sql
         https://stackoverflow.com/users/3058123/sbha
      5. SAS sql arrays

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

data have;
 input
   V1 V2 V3;
cards4;
2  7  9
8  3  6
1  5  4
;;;;
run;quit;

/**************************************************************************************************************************/
/*                         |                                                                                              */
/* WORK.HAVE total obs=3   |    RULES                                                                                     */
/*                         |                   CREATE THIS                                                                */
/* Obs    V1    V2    V3   |     V1 V2 V3      COL_MAX                                                                    */
/*                         |                                                                                              */
/*  1      2     7     9   |      2  7  9        V3      =9  largest value in the row                                     */
/*  2      8     3     6   |      8  3  6        V1      =8  largest value in the row                                     */
/*  3      1     5     4   |      1  5  4        V2      =5  largest value in the row                                     */
/*                         |                                                                                              */
/**************************************************************************************************************************/

/*
  ___                             _                    ___
 / _ \    _ __   ___    ___  __ _| |  ___  __ _ ___   ( _ )    __      ___ __  ___
| | | |  | `_ \ / _ \  / __|/ _` | | / __|/ _` / __|  / _ \/\  \ \ /\ / / `_ \/ __|
| |_| |  | | | | (_) | \__ \ (_| | | \__ \ (_| \__ \ | (_>  <   \ V  V /| |_) \__ \
 \___(_) |_| |_|\___/  |___/\__, |_| |___/\__,_|___/  \___/\/    \_/\_/ | .__/|___/
                               |_|                                      |_|
*/
/*
 ___  __ _ ___
/ __|/ _` / __|
\__ \ (_| \__ \
|___/\__,_|___/

*/
data want;
  set have;
   array vs (3) v1-v3;
   max_v=max(of v1-v3);
   var=vname(vs(whichn(max_v,of vs(*))));
run;
/*
__      ___ __  ___
\ \ /\ / / `_ \/ __|
 \ V  V /| |_) \__ \
  \_/\_/ | .__/|___/
         |_|
*/
%let _pth=%sysfunc(pathname(work));
%utl_submit_wps64('
libname wrk "&_pth";
data want;
  set wrk.have;
   array vs (3) v1-v3;
   max_v=max(of v1-v3);
   var=vname(vs(whichn(max_v,of vs(*))));
run;quit;
proc print data=want;
run;quit;
');


/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs from WANT total obs=3 02JUN2023:14:30:16                                                                  */
/*                                                                                                                        */
/* Obs    V1    V2    V3    MAX_V    VAR                                                                                  */
/*                                                                                                                        */
/*  1      2     7     9      9      V3                                                                                   */
/*  2      8     3     6      8      V1                                                                                   */
/*  3      1     5     4      5      V2                                                                                   */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                             _
/ |    _ __   ___    ___  __ _| |  _ __
| |   | `_ \ / _ \  / __|/ _` | | | `__|
| |_  | | | | (_) | \__ \ (_| | | | |
|_(_) |_| |_|\___/  |___/\__, |_| |_|
                            |_|
*/


%let _pth=%sysfunc(pathname(work));
%utl_submit_wps64('
libname wrk "&_pth";
proc r;
export data=wrk.have r=have;
submit;
library(tidyverse);
have;
have %>%
  rownames_to_column("id") %>%
  gather(dept, cnt, V1:V3) %>%
  group_by(id) %>%
  mutate(dept_rank  = rank(-cnt, ties.method = "first")) %>%
  filter(dept_rank == 1) %>%
  select(-dept_rank);
endsubmit;
');

/*           _   _                     _           _               _
 _ __  _   _| |_| |__   ___  _ __     (_)_   _ ___| |_   ___  __ _| |
| `_ \| | | | __| `_ \ / _ \| `_ \    | | | | / __| __| / __|/ _` | |
| |_) | |_| | |_| | | | (_) | | | |   | | |_| \__ \ |_  \__ \ (_| | |
| .__/ \__, |\__|_| |_|\___/|_| |_|  _/ |\__,_|___/\__| |___/\__, |_|
|_|    |___/                        |__/                        |_|
*/


libname sd1 "d:/sd1";

data sd1.have;
 input
   V1 V2 V3;
cards4;
2  7  9
8  3  6
1  5  4
;;;;
run;quit;

proc datasets lib=work kill nodetails nolist;
run;quit;

%utlfkil(d:/xpt/res.xpt);

%utl_pybegin;
parmcards4;
from os import path
import pandas as pd
import xport
import xport.v56
import pyreadstat
import numpy as np
import pandas as pd
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
have, meta = pyreadstat.read_sas7bdat("d:/sd1/have.sas7bdat")
print(have);
res = pdsql("""
  select
      row
     ,var
      ,max(val) as maxVal
   from
  (
   select
     row   as row
    ,'V1'  as var
    ,v1    as val
   from
     (select ROW_NUMBER() OVER (ORDER BY V1-V3) as row, * from have)
  union
    all
  select
     row   as row
    ,'V2'  as var
    ,v2    as val
   from
     (select ROW_NUMBER() OVER (ORDER BY V1-V3) as row, * from have)
  union
    all
  select
     row   as row
    ,'V3'  as var
    ,v3    as val
   from
     (select ROW_NUMBER() OVER (ORDER BY V1-V3) as row, * from have)
  )
  group
    by row
  having
    val = max(val)
  order
    by row
  """)
print(res);
ds = xport.Dataset(res, name='res')
with open('d:/xpt/res.xpt', 'wb') as f:
    xport.v56.dump(ds, f)
;;;;
%utl_pyend;

libname pyxpt xport "d:/xpt/res.xpt";

proc contents data=pyxpt._all_;
run;quit;

proc print data=pyxpt.res;
run;quit;

data res;
   set pyxpt.res;
run;quit;

/**************************************************************************************************************************/
/*                          |                                                                                             */
/*  PYTHON                  |  SAS IMPORT                                                                                 */
/*                          |                                                                                             */
/*                          |   Up to 40 obs from RES total obs=3 02JUN2023:12:01:04                                      */
/*                          |                                                                                             */
/*      row var  maxVal     |   Obs    ROW    VAR    MAXVAL                                                               */
/*                          |                                                                                             */
/*   0    1  V3     9.0     |    1      1     V3        9                                                                 */
/*   1    2  V2     5.0     |    2      2     V2        5                                                                 */
/*   2    3  V1     8.0     |    3      3     V1        8                                                                 */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                                   _
|___ /   __      ___ __  ___   ___  __ _| |   __ _ _ __ _ __ __ _ _   _
  |_ \   \ \ /\ / / `_ \/ __| / __|/ _` | |  / _` | `__| `__/ _` | | | |
 ___) |   \ V  V /| |_) \__ \ \__ \ (_| | | | (_| | |  | | | (_| | |_| |
|____(_)   \_/\_/ | .__/|___/ |___/\__, |_|  \__,_|_|  |_|  \__,_|\__, |
                  |_|                 |_|                         |___/
*/
%utl_submit_wps64('
/*--- I repalced single quotes with double quotes for a single quoted drop down ----*/
options validvarname=any sasautos=("c:/otowps") ;
libname sd1 "d:/sd1";
%array(_vars,values=1-3);
%put &_varsn;
proc sql;

  create
    table want as
  select
      row
     ,var
      ,max(val) as maxVal
   from
  (
   %do_over(_vars,phrase=%str(
    select
      row   as row
     ,"V?"   as var
     ,V?    as val
    from
      (select monotonic() as row, * from sd1.have)
      ),between=union all )
  )
  group
    by row
  having
    val = max(val)
  order
    by row
;quit;
run;quit;
proc print data=want;
run;quit;
',resolve=N);

/*           _               _
  ___  _   _| |_ _ __  _   _| |_
 / _ \| | | | __| `_ \| | | | __|
| (_) | |_| | |_| |_) | |_| | |_
 \___/ \__,_|\__| .__/ \__,_|\__|
                |_|
*/
/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                      max                                                                                               */
/* Obs    row    var    Val                                                                                               */
/*                                                                                                                        */
/*  1      1     V3      9                                                                                                */
/*  2      2     V1      8                                                                                                */
/*  3      3     V2      5                                                                                                */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*  _                                            _     _  __
| || |    __      ___ __  ___   _ __   ___  __ _| | __| |/ _|
| || |_   \ \ /\ / / `_ \/ __| | `__| / __|/ _` | |/ _` | |_
|__   _|   \ V  V /| |_) \__ \ | |    \__ \ (_| | | (_| |  _|
   |_|(_)   \_/\_/ | .__/|___/ |_|    |___/\__, |_|\__,_|_|
                   |_|                        |_|
*/

data have;
 input
   V1 V2 V3;
cards4;
2  7  9
8  3  6
1  5  4
;;;;
run;quit;

%let _pth=%sysfunc(pathname(work));
%utl_submit_wps64('
libname wrk "&_pth";
 proc r;
 export data=wrk.have r=have;
 submit;
 library(sqldf);
 want_r_sqldf<-sqldf("
  select
      row
     ,var
      ,max(val) as maxVal
   from
  (
   select
     row   as row
    ,\"V1\"  as var
    ,v1    as val
   from
     (select ROW_NUMBER() OVER (ORDER BY V1-V3) as row, * from have)
  union
    all
  select
     row   as row
    ,\"V2\"  as var
    ,v2    as val
   from
     (select ROW_NUMBER() OVER (ORDER BY V1-V3) as row, * from have)
  union
    all
  select
     row   as row
    ,\"V3\"  as var
    ,v3    as val
   from
     (select ROW_NUMBER() OVER (ORDER BY V1-V3) as row, * from have)
  )
  group
    by row
  having
    val = max(val)
  order
    by row
 ");
 want_r_sqldf;
 endsubmit;
 import data=wrk.want_r_sqldf r=want_r_sqldf;
');

proc print data=want_r_sqldf;
run;quit;

/**************************************************************************************************************************/
/*                  |                                                                                                     */
/*       WPS        |   SAS  IMPORT                                                                                       */
/*                  |                                                                                                     */
/*                  |   Up to 40 obs from WANT_R_SQLDF total obs=&tob 02JUN2023:13:47:19                                  */
/* The WPS System   |                                                                                                     */
/*                  |   Obs    ROW    VAR    MAXVAL                                                                       */
/*   row var maxVal |                                                                                                     */
/* 1   1   9      9 |    1      1      9        9                                                                         */
/* 2   2   5      5 |    2      2      5        5                                                                         */
/* 3   3   8      8 |    3      3      8        8                                                                         */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                               _
| ___|    ___  __ _ ___   ___  __ _| |   __ _ _ __ _ __ __ _ _   _
|___ \   / __|/ _` / __| / __|/ _` | |  / _` | `__| `__/ _` | | | |
 ___) |  \__ \ (_| \__ \ \__ \ (_| | | | (_| | |  | | | (_| | |_| |
|____(_) |___/\__,_|___/ |___/\__, |_|  \__,_|_|  |_|  \__,_|\__, |
                                 |_|                         |___/
*/

%array(_vars,values=1-3);

%put &=_varsn;

proc sql;

  create
    table want as
  select
      row
     ,var
      ,max(val) as maxVal
   from
  (
   %do_over(_vars,phrase=%str(
    select
      row   as row
     ,"V?"   as var
     ,V?    as val
    from
      (select monotonic() as row, * from have)
      ),between=union all )
  )
  group
    by row
  having
    val = max(val)
  order
    by row
;quit;

if you want the generated code

data _null;
  %do_over(_vars,phrase=%str(
    put "select row as row ,'v? ' as var ,V? as val from (select monotonic() as row, * from have, between=union all";));
 run;quit;

select row as row ,'v1 ' as var ,V1 as val from (select monotonic() as row, * from have, between=union all
select row as row ,'v2 ' as var ,V2 as val from (select monotonic() as row, * from have, between=union all
select row as row ,'v3 ' as var ,V3 as val from (select monotonic() as row, * from have, between=union all

/*           _               _
  ___  _   _| |_ _ __  _   _| |_
 / _ \| | | | __| `_ \| | | | __|
| (_) | |_| | |_| |_) | |_| | |_
 \___/ \__,_|\__| .__/ \__,_|\__|
                |_|
*/

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs from last table WORK.WANT total obs=3 02JUN2023:11:56:                                                    */
/*                                                                                                                        */
/* Obs    ROW    VAR    MAXVAL                                                                                            */
/*                                                                                                                        */
/*  1      1     V3        9                                                                                              */
/*  2      2     V1        8                                                                                              */
/*  3      3     V2        5                                                                                              */
/*                                                                                                                        */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
