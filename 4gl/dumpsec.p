/* dumpsec.p - dump de segurança
   PARAM: diretório onde salvar
*/

DEFINE VARIABLE cDir    AS CHARACTER NO-UNDO.
DEFINE VARIABLE cLogDir AS CHARACTER NO-UNDO.
DEFINE VARIABLE cLog    AS CHARACTER NO-UNDO.

cDir = SESSION:PARAMETER.

IF cDir = ? OR cDir = "" THEN DO:
  OUTPUT TO VALUE("dumpsec.error.log") APPEND.
  PUT UNFORMATTED "ERRO: dumpsec.p sem parametro" SKIP.
  OUTPUT CLOSE.
  QUIT.
END.

cLogDir = cDir + "/logs".
OS-CREATE-DIR VALUE(cLogDir).
cLog = cLogDir + "/00_dumpsec_put.log".

OUTPUT TO VALUE(cLog) APPEND.
PUT UNFORMATTED "=== INICIO dumpsec.p ===" SKIP.
PUT UNFORMATTED "DIR: " cDir SKIP.

SESSION:APPL-ALERT-BOXES = NO.

RUN prodict/dump_d.p (
  "_sec-role,_sec-granted-role,_sec-granted-role-condition",
  cDir + "/",
  "ISO8859-1"
).

PUT UNFORMATTED "=== FIM dumpsec.p ===" SKIP.
OUTPUT CLOSE.

QUIT.

