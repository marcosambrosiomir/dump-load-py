/* dumpdf.p - batch-safe (compatível)
   PARAM: pasta de saída do dump
*/
DEFINE VARIABLE cDir AS CHARACTER NO-UNDO.
DEFINE VARIABLE cDf  AS CHARACTER NO-UNDO.

cDir = SESSION:PARAMETER.

IF cDir = ? OR cDir = "" THEN DO:
  QUIT.
END.

cDf = ENTRY(NUM-ENTRIES(cDir, "/"), cDir, "/") + ".df".

SESSION:APPL-ALERT-BOXES = NO.

RUN prodict/dump_df.p (
  "ALL",
  cDir + "/" + cDf,
  "ISO8859-1"
).

QUIT.

