/* dumpseq.p - dump de sequências (batch-safe)
   PARAM: diretório onde será salvo seqvals.d
*/

DEFINE VARIABLE cDir    AS CHARACTER NO-UNDO.
DEFINE VARIABLE cLogDir AS CHARACTER NO-UNDO.
DEFINE VARIABLE cLog    AS CHARACTER NO-UNDO.
DEFINE VARIABLE hDump   AS HANDLE    NO-UNDO.

cDir = SESSION:PARAMETER.

IF cDir = ? OR cDir = "" THEN DO:
  OUTPUT TO VALUE("dumpseq.error.log") APPEND.
  PUT UNFORMATTED "ERRO: dumpseq.p sem parametro" SKIP.
  OUTPUT CLOSE.
  QUIT.
END.

cLogDir = cDir + "/logs".
OS-CREATE-DIR VALUE(cLogDir).
cLog = cLogDir + "/00_dumpseq_put.log".

OUTPUT TO VALUE(cLog) APPEND.
PUT UNFORMATTED "=== INICIO dumpseq.p ===" SKIP.
PUT UNFORMATTED "DIR: " cDir SKIP.

SESSION:APPL-ALERT-BOXES = NO.

/* dump de sequências */
RUN prodict/dmpseqvals.p PERSISTENT SET hDump.
RUN setFileName IN hDump (cDir + "/seqvals.d").
RUN doDump IN hDump.
DELETE PROCEDURE hDump.

PUT UNFORMATTED "=== FIM dumpseq.p ===" SKIP.
OUTPUT CLOSE.

QUIT.

