/* dumpuser.p - dump da tabela _User
   PARAM: diretório onde salvar
*/

DEFINE VARIABLE cDir    AS CHARACTER NO-UNDO.
DEFINE VARIABLE cLogDir AS CHARACTER NO-UNDO.
DEFINE VARIABLE cLog    AS CHARACTER NO-UNDO.

cDir = SESSION:PARAMETER.

IF cDir = ? OR cDir = "" THEN DO:
  OUTPUT TO VALUE("dumpuser.error.log") APPEND.
  PUT UNFORMATTED "ERRO: dumpuser.p sem parametro" SKIP.
  OUTPUT CLOSE.
  QUIT.
END.

cLogDir = cDir + "/logs".
OS-CREATE-DIR VALUE(cLogDir).
cLog = cLogDir + "/00_dumpuser_put.log".

OUTPUT TO VALUE(cLog) APPEND.
PUT UNFORMATTED "=== INICIO dumpuser.p ===" SKIP.
PUT UNFORMATTED "DIR: " cDir SKIP.

SESSION:APPL-ALERT-BOXES = NO.

/* codepage correto */
RUN prodict/dump_d.p (
  "_User",
  cDir + "/",
  "ISO8859-1"
).

PUT UNFORMATTED "=== FIM dumpuser.p ===" SKIP.
OUTPUT CLOSE.

QUIT.

