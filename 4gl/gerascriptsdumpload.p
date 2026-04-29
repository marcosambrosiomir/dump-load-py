DEFINE VARIABLE cDB        AS CHARACTER NO-UNDO.
DEFINE VARIABLE i          AS INTEGER   NO-UNDO.
DEFINE VARIABLE cParams    AS CHARACTER NO-UNDO.

DEFINE VARIABLE PATH_SCRIPTS AS CHARACTER NO-UNDO.
DEFINE VARIABLE PATH_DB    AS CHARACTER NO-UNDO.
DEFINE VARIABLE PATH_LOAD  AS CHARACTER NO-UNDO.
DEFINE VARIABLE PATHDUMP   AS CHARACTER NO-UNDO.
DEFINE VARIABLE PATH_TEMP  AS CHARACTER NO-UNDO.
DEFINE VARIABLE DLC        AS CHARACTER NO-UNDO.
DEFINE VARIABLE DL_ONLINE  AS CHARACTER NO-UNDO.

cParams = SESSION:PARAMETER.
IF cParams = ? OR cParams = "" THEN DO:
  MESSAGE "ERRO: parametros ausentes para gerascriptsdumpload.p" VIEW-AS ALERT-BOX ERROR.
  QUIT.
END.

IF NUM-ENTRIES(cParams, "|") <> 7 THEN DO:
  MESSAGE "ERRO: parametros invalidos para gerascriptsdumpload.p" VIEW-AS ALERT-BOX ERROR.
  QUIT.
END.

PATH_SCRIPTS = ENTRY(1, cParams, "|").
PATH_DB = ENTRY(2, cParams, "|").
PATH_LOAD = ENTRY(3, cParams, "|").
PATHDUMP = ENTRY(4, cParams, "|").
PATH_TEMP = ENTRY(5, cParams, "|").
DLC = ENTRY(6, cParams, "|").
DL_ONLINE = ENTRY(7, cParams, "|").

OS-CREATE-DIR VALUE(PATHDUMP).
OS-CREATE-DIR VALUE(PATH_LOAD).
OS-CREATE-DIR VALUE(PATH_TEMP).

DO i = 1 TO NUM-DBS:

  CREATE ALIAS "DICTDB" FOR DATABASE VALUE(LDBNAME(i)).
  cDB = PDBNAME("DICTDB").

  OS-CREATE-DIR VALUE(PATHDUMP + "/" + cDB).
  OS-CREATE-DIR VALUE(PATHDUMP + "/" + cDB + "/logs").

  /* ========================= dump-<db>.sh ========================= */
  OUTPUT TO VALUE(PATHDUMP + "/" + cDB + "/dump-" + cDB + ".sh").

  PUT UNFORMATTED "#!/bin/sh" SKIP.
  PUT UNFORMATTED "set -e" SKIP.
  PUT UNFORMATTED "export DLC=" DLC SKIP.
  PUT UNFORMATTED "export PROCFG=" PATH_SCRIPTS "/progress.cfg" SKIP.
  PUT UNFORMATTED "export PATH=$DLC/bin:$PATH" SKIP.
  PUT UNFORMATTED "export TERM=xterm" SKIP.
  PUT UNFORMATTED "export PROTERMCAP=$DLC/protermcap" SKIP.
  PUT UNFORMATTED ". " PATH_SCRIPTS "/common-monitor.sh" SKIP.
  PUT UNFORMATTED "DL_ONLINE=" DL_ONLINE SKIP.
  PUT UNFORMATTED "DBNAME=" cDB SKIP.
  PUT UNFORMATTED "DBORIG=" PATH_DB "/" cDB SKIP.
  PUT UNFORMATTED "DUMPDIR=" PATHDUMP "/" cDB SKIP.
  PUT UNFORMATTED "if [ \"$DL_ONLINE\" = \"1\" ]; then" SKIP.
  PUT UNFORMATTED "  DBORIG_OPTS=\"-1 -b\"" SKIP.
  PUT UNFORMATTED "else" SKIP.
  PUT UNFORMATTED "  DBORIG_OPTS=\"-b\"" SKIP.
  PUT UNFORMATTED "fi" SKIP.
  PUT UNFORMATTED "if [ \"$DL_ONLINE\" = \"1\" ]; then" SKIP.
  PUT UNFORMATTED "  DBDEST_OPTS=\"-1 -b\"" SKIP.
  PUT UNFORMATTED "else" SKIP.
  PUT UNFORMATTED "  DBDEST_OPTS=\"-b\"" SKIP.
  PUT UNFORMATTED "fi" SKIP.
  PUT UNFORMATTED "LOGDIR=$DUMPDIR/logs" SKIP.
  PUT UNFORMATTED "mkdir -p $LOGDIR" SKIP.
  PUT UNFORMATTED "TS=$(date +%Y%m%d-%H%M%S)" SKIP.
  PUT UNFORMATTED "MASTER=$LOGDIR/dump-$DBNAME-$TS.log" SKIP.
  PUT UNFORMATTED "" SKIP.

  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo DERRUBA_PROD:proshut >>$MASTER" SKIP.
  PUT UNFORMATTED "if [ \"$DL_ONLINE\" = \"1\" ]; then" SKIP.
  PUT UNFORMATTED "  $DLC/bin/proshut -by $DBORIG >>$MASTER 2>&1 || true" SKIP.
  PUT UNFORMATTED "else" SKIP.
  PUT UNFORMATTED "  echo IGNORADO:proshut DL_ONLINE=0 >>$MASTER" SKIP.
  PUT UNFORMATTED "fi" SKIP.
  PUT UNFORMATTED "echo OK:proshut >>$MASTER" SKIP.
  PUT UNFORMATTED "" SKIP.
     
  /* pré */
  PUT UNFORMATTED "rm -f $DUMPDIR/$DBNAME.df" SKIP.
  PUT UNFORMATTED "rm -f $DUMPDIR/seqvals.d" SKIP.
  PUT UNFORMATTED "rm -f $DUMPDIR/_User.d" SKIP.
  PUT UNFORMATTED "rm -f $DUMPDIR/_sec-role.d $DUMPDIR/_sec-granted-role.d $DUMPDIR/_sec-granted-role-condition.d" SKIP.
  PUT UNFORMATTED "" SKIP.

  /* DF */
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo START:dump_df >>$MASTER" SKIP.
  PUT UNFORMATTED "run_step DUMP_DF $DLC/bin/_progres -db $DBORIG $DBORIG_OPTS -logfile $LOGDIR/00_dumpdf.log -p " PATH_SCRIPTS "/dumpdf.p -param $DUMPDIR/$DBNAME.df >>$MASTER 2>&1 || exit 1" SKIP.
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo OK:dump_df >>$MASTER" SKIP.
  PUT UNFORMATTED "" SKIP.

  /* SEQ */
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo START:dump_seq >>$MASTER" SKIP.
  PUT UNFORMATTED "run_step DUMP_SEQ $DLC/bin/_progres -db $DBORIG $DBORIG_OPTS -logfile $LOGDIR/00_dumpseq.log -p " PATH_SCRIPTS "/dumpseq.p -param $DUMPDIR >>$MASTER 2>&1 || exit 1" SKIP.
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo OK:dump_seq >>$MASTER" SKIP.
  PUT UNFORMATTED "" SKIP.

  /* USER */
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo START:dump_user >>$MASTER" SKIP.
  PUT UNFORMATTED "run_step DUMP_USER $DLC/bin/_progres -db $DBORIG $DBORIG_OPTS -logfile $LOGDIR/00_dumpuser.log -p " PATH_SCRIPTS "/dumpuser.p -param $DUMPDIR >>$MASTER 2>&1 || exit 1" SKIP.
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo OK:dump_user >>$MASTER" SKIP.
  PUT UNFORMATTED "" SKIP.

  /* SEC */
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo START:dump_sec >>$MASTER" SKIP.
  PUT UNFORMATTED "run_step DUMP_SEC $DLC/bin/_progres -db $DBORIG $DBORIG_OPTS -logfile $LOGDIR/00_dumpsec.log -p " PATH_SCRIPTS "/dumpsec.p -param $DUMPDIR >>$MASTER 2>&1 || exit 1" SKIP.
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo OK:dump_sec >>$MASTER" SKIP.
  PUT UNFORMATTED "" SKIP.
 
  /* dump binário por tabela */
  FOR EACH DICTDB._File NO-LOCK
    WHERE DICTDB._File._File-num > 0
      AND DICTDB._File._File-num < 32768:

    PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
    PUT UNFORMATTED "echo START:dump_" DICTDB._File._File-name " >>$MASTER" SKIP.
    PUT UNFORMATTED "mon_table ""DUMP"" """ DICTDB._File._File-name """" SKIP. 
    PUT UNFORMATTED
      "$DLC/bin/proutil $DBORIG -i -C dump "
      DICTDB._File._File-name
      " $DUMPDIR -T "
      PATH_TEMP
      " >>$MASTER 2>&1 || exit 1"
      SKIP.
    PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
    PUT UNFORMATTED "echo OK:dump_" DICTDB._File._File-name " >>$MASTER" SKIP.
    PUT UNFORMATTED "" SKIP.
  END.
 
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo START:tabanalys inicio >>$MASTER" SKIP.
  PUT UNFORMATTED "run_step TABANALYS_INI $DLC/bin/proutil  $DBORIG -C tabanalys -s 8192 -T " PATH_TEMP " >> "  PATH_LOAD  "/"  cDB  "_tab.ini || exit 1" SKIP.
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo OK:tabanalys inicio >>$MASTER" SKIP.


 
  OUTPUT CLOSE.

  /* ========================= load-<db>.sh ========================= */
  OUTPUT TO VALUE(PATHDUMP + "/" + cDB + "/load-" + cDB + ".sh").

  PUT UNFORMATTED "#!/bin/sh" SKIP.
  PUT UNFORMATTED "set -e" SKIP.
  PUT UNFORMATTED "export DLC=" DLC SKIP.
  PUT UNFORMATTED "export PROCFG=" PATH_SCRIPTS "/progress.cfg" SKIP.
  PUT UNFORMATTED "export PATH=$DLC/bin:$PATH" SKIP.
  PUT UNFORMATTED "export TERM=xterm" SKIP.
  PUT UNFORMATTED "export PROTERMCAP=$DLC/protermcap" SKIP.
  PUT UNFORMATTED ". " PATH_SCRIPTS "/common-monitor.sh" SKIP.
  PUT UNFORMATTED "DL_ONLINE=" DL_ONLINE SKIP.
  PUT UNFORMATTED "DBNAME=" cDB SKIP.
  PUT UNFORMATTED "DBDEST=" PATH_LOAD "/" cDB SKIP.
  PUT UNFORMATTED "DUMPDIR=" PATHDUMP "/" cDB SKIP.
  PUT UNFORMATTED "LOGDIR=$DUMPDIR/logs" SKIP.
  PUT UNFORMATTED "mkdir -p $LOGDIR" SKIP.
  PUT UNFORMATTED "TS=$(date +%Y%m%d-%H%M%S)" SKIP.
  PUT UNFORMATTED "MASTER=$LOGDIR/load-$DBNAME-$TS.log" SKIP.
  PUT UNFORMATTED "" SKIP.

  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo START:proshut >>$MASTER" SKIP.
  PUT UNFORMATTED "if [ \"$DL_ONLINE\" = \"1\" ]; then" SKIP.
  PUT UNFORMATTED "  $DLC/bin/proshut -by $DBDEST >>$MASTER 2>&1 || true" SKIP.
  PUT UNFORMATTED "else" SKIP.
  PUT UNFORMATTED "  echo IGNORADO:proshut DL_ONLINE=0 >>$MASTER" SKIP.
  PUT UNFORMATTED "fi" SKIP.
  PUT UNFORMATTED "echo OK:proshut >>$MASTER" SKIP.
  PUT UNFORMATTED "" SKIP.

  PUT UNFORMATTED "cd " PATH_LOAD SKIP.
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo START:procopy >>$MASTER" SKIP.
  PUT UNFORMATTED "run_step PROCOPY echo y | $DLC/bin/procopy $DLC/empty8 $DBNAME >>$MASTER 2>&1 || exit 1" SKIP.
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo OK:procopy >>$MASTER" SKIP.
  PUT UNFORMATTED "" SKIP.

  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo START:loaddf >>$MASTER" SKIP.
  PUT UNFORMATTED "run_step LOAD_DF $DLC/bin/_progres -db $DBDEST $DBDEST_OPTS -logfile $LOGDIR/01_loaddf.log -p " PATH_SCRIPTS "/loaddf.p -param $DUMPDIR/$DBNAME.df >>$MASTER 2>&1 || exit 1" SKIP.
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo OK:loaddf >>$MASTER" SKIP.
  PUT UNFORMATTED "" SKIP.

  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo START:loadd >>$MASTER" SKIP.
  PUT UNFORMATTED "run_step LOAD_DATA $DLC/bin/_progres -db $DBDEST $DBDEST_OPTS -logfile $LOGDIR/02_loadd.log -p " PATH_SCRIPTS "/loadd.p -param $DUMPDIR >>$MASTER 2>&1 || exit 1" SKIP.
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo OK:loadd >>$MASTER" SKIP.
  PUT UNFORMATTED "" SKIP.

  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo START:loadseq >>$MASTER" SKIP.
  PUT UNFORMATTED "run_step LOAD_SEQ $DLC/bin/_progres -db $DBDEST $DBDEST_OPTS -logfile $LOGDIR/03_loadseq.log -p " PATH_SCRIPTS "/loadseq.p -param $DUMPDIR/ >>$MASTER 2>&1 || exit 1" SKIP.
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo OK:loadseq >>$MASTER" SKIP.
  PUT UNFORMATTED "" SKIP.

  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo START:loaduser >>$MASTER" SKIP.
  PUT UNFORMATTED "run_step LOAD_USER $DLC/bin/_progres -db $DBDEST $DBDEST_OPTS -logfile $LOGDIR/04_loaduser.log -p " PATH_SCRIPTS "/loaduser.p -param $DUMPDIR/_User.d >>$MASTER 2>&1 || exit 1" SKIP.
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo OK:loaduser >>$MASTER" SKIP.
  PUT UNFORMATTED "" SKIP.
  
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo START:tablemove >>$MASTER" SKIP.
  PUT UNFORMATTED "run_step TABLEMOVE bash " PATHDUMP + "/" + cDB + "/tablemove_" + cDB + ".sh >>$MASTER 2>&1 || true" SKIP.
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo OK:tablemove >>$MASTER" SKIP.
    
  /* load binário por tabela */
  FOR EACH DICTDB._File NO-LOCK
    WHERE DICTDB._File._File-num > 0
      AND DICTDB._File._File-num < 32768:

    PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
    PUT UNFORMATTED "echo START:load_" DICTDB._File._File-name " >>$MASTER" SKIP.
    PUT UNFORMATTED "mon_table ""LOAD"" """ DICTDB._File._File-name """" SKIP.
    PUT UNFORMATTED
      "$DLC/bin/proutil $DBDEST -i -C load $DUMPDIR/"
      DICTDB._File._File-name
      ".bd -T "
      PATH_TEMP
      " >>$MASTER 2>&1 || exit 1"
      SKIP.
    PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
    PUT UNFORMATTED "echo OK:load_" DICTDB._File._File-name " >>$MASTER" SKIP.
    PUT UNFORMATTED "" SKIP.
  END.

  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo START:idxbuild >>$MASTER" SKIP.
  PUT UNFORMATTED "run_step IDXBUILD $DLC/bin/proutil $DBDEST -C idxbuild all   -SG 16 -TB 64 -TM 32 -TMB 128 -B 16000 -TF 88 -thread 1 -threadnum 6 -mergethreads 4 -datascanthreads 4 -pfactor 90 -rusage -T " PATH_TEMP " >>$MASTER 2>&1 || exit 1" SKIP.
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo OK:idxbuild >>$MASTER" SKIP.

  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo START:tabanalys fim >>$MASTER" SKIP.
  PUT UNFORMATTED "run_step TABANALYS_FIM $DLC/bin/proutil $DBDEST -C tabanalys -s 8192 -T " PATH_TEMP " >> "  PATH_LOAD  "/"  cDB  "_tab.fim || exit 1" SKIP.
  PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
  PUT UNFORMATTED "echo OK:tabanalys fim >>$MASTER" SKIP.

  
  OUTPUT CLOSE.

   /* ========================= Table Move.sh ========================= */
  OUTPUT TO VALUE(PATHDUMP + "/" + cDB + "/tablemove_" + cDB + ".sh").

  PUT UNFORMATTED "#!/bin/sh" SKIP.
  PUT UNFORMATTED "set -e" SKIP.
  PUT UNFORMATTED "export DLC=" DLC SKIP.
  PUT UNFORMATTED "export PROCFG=" PATH_SCRIPTS "/progress.cfg" SKIP.
  PUT UNFORMATTED "export PATH=$DLC/bin:$PATH" SKIP.
  PUT UNFORMATTED "export TERM=xterm" SKIP.
  PUT UNFORMATTED "export PROTERMCAP=$DLC/protermcap" SKIP.
  PUT UNFORMATTED "DBNAME=" cDB SKIP.
  PUT UNFORMATTED "DBDEST=" PATH_LOAD "/" cDB SKIP.
  PUT UNFORMATTED "DUMPDIR=" PATHDUMP "/" cDB SKIP.
  PUT UNFORMATTED "LOGDIR=$DUMPDIR/logs" SKIP.
  PUT UNFORMATTED "mkdir -p $LOGDIR" SKIP.
  PUT UNFORMATTED "TS=$(date +%Y%m%d-%H%M%S)" SKIP.
  PUT UNFORMATTED "MASTER=$LOGDIR/tablemove-$DBNAME-$TS.log" SKIP.
  PUT UNFORMATTED "" SKIP.

   /* load binário por tabela */
  FOR EACH DICTDB._File NO-LOCK
    WHERE DICTDB._File._File-num > 0
      AND DICTDB._File._File-num < 32768:

    PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
    PUT UNFORMATTED "echo START:tablemove_" DICTDB._File._File-name " >>$MASTER" SKIP.
    PUT UNFORMATTED
      "$DLC/bin/proutil $DBDEST -C tablemove " 
      DICTDB._File._File-name
      " Dados Indices -T " 
      PATH_TEMP
      " >>$MASTER 2>&1 || exit 1"
      SKIP.
    PUT UNFORMATTED "date +%F_%T >>$MASTER" SKIP.
    PUT UNFORMATTED "echo OK:tablemove_" DICTDB._File._File-name " >>$MASTER" SKIP.
    PUT UNFORMATTED "" SKIP.
  END.

  OUTPUT CLOSE.
OS-COMMAND NO-WAIT VALUE("chmod 755 " + PATHDUMP + "/" + cDB + "/*.sh").

  DELETE ALIAS "DICTDB".
END.

QUIT.
