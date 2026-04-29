/* loadsec.p - load de segurança
	 PARAM: diretório onde estão os arquivos de segurança
*/

DEFINE VARIABLE cDir    AS CHARACTER NO-UNDO.
DEFINE VARIABLE cLogDir AS CHARACTER NO-UNDO.
DEFINE VARIABLE cLog    AS CHARACTER NO-UNDO.

cDir = SESSION:PARAMETER.

IF cDir = ? OR cDir = "" THEN DO:
	OUTPUT TO VALUE("loadsec.error.log") APPEND.
	PUT UNFORMATTED "ERRO: loadsec.p sem parametro" SKIP.
	OUTPUT CLOSE.
	QUIT.
END.

cLogDir = cDir + "/logs".
OS-CREATE-DIR VALUE(cLogDir).
cLog = cLogDir + "/05_loadsec_put.log".

OUTPUT TO VALUE(cLog) APPEND.
PUT UNFORMATTED "=== INICIO loadsec.p ===" SKIP.
PUT UNFORMATTED "DIR: " cDir SKIP.

SESSION:APPL-ALERT-BOXES = NO.

RUN prodict/load_d.p (
	"_sec-role,_sec-granted-role,_sec-granted-role-condition",
	INPUT cDir
).

PUT UNFORMATTED "=== FIM loadsec.p ===" SKIP.
OUTPUT CLOSE.

QUIT.