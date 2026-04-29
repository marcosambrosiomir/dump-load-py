/* dumptbl.p - inventário de tabelas
   PARAM: diretório do banco onde será salvo tables.lst
*/

DEFINE VARIABLE cDir    AS CHARACTER NO-UNDO.
DEFINE VARIABLE cLogDir AS CHARACTER NO-UNDO.
DEFINE VARIABLE cLog    AS CHARACTER NO-UNDO.
DEFINE VARIABLE cLst    AS CHARACTER NO-UNDO.

cDir = SESSION:PARAMETER.

IF cDir = ? OR cDir = "" THEN DO:
  OUTPUT TO VALUE("dumptbl.error.log") APPEND.
  PUT UNFORMATTED "ERRO: dumptbl.p sem parametro" SKIP.
  OUTPUT CLOSE.
  QUIT.
END.

cLogDir = cDir + "/logs".
OS-CREATE-DIR VALUE(cLogDir).
cLog = cLogDir + "/00_dumptbl_put.log".
cLst = cDir + "/tables.lst".

OUTPUT TO VALUE(cLog) APPEND.
PUT UNFORMATTED "=== INICIO dumptbl.p ===" SKIP.
PUT UNFORMATTED "DIR: " cDir SKIP.

OUTPUT TO VALUE(cLst).

FOR EACH DICTDB._File NO-LOCK
  WHERE DICTDB._File._File-num > 0
    AND DICTDB._File._File-num < 32768:
  PUT UNFORMATTED DICTDB._File._File-name SKIP.
END.

OUTPUT CLOSE.

OUTPUT TO VALUE(cLog) APPEND.
PUT UNFORMATTED "=== FIM dumptbl.p ===" SKIP.
OUTPUT CLOSE.

QUIT.
 