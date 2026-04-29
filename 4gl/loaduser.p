/* loaduser.p - load da tabela _User
	 PARAM: caminho completo do arquivo _User.d
*/

DEFINE VARIABLE cFile   AS CHARACTER NO-UNDO.
DEFINE VARIABLE cLogDir AS CHARACTER NO-UNDO.
DEFINE VARIABLE cLog    AS CHARACTER NO-UNDO.
DEFINE VARIABLE iSlash  AS INTEGER   NO-UNDO.

cFile = SESSION:PARAMETER.

IF cFile = ? OR cFile = "" THEN DO:
	OUTPUT TO VALUE("loaduser.error.log") APPEND.
	PUT UNFORMATTED "ERRO: loaduser.p sem parametro" SKIP.
	OUTPUT CLOSE.
	QUIT.
END.

iSlash = R-INDEX(cFile, "/").
IF iSlash > 0 THEN
	cLogDir = SUBSTRING(cFile, 1, iSlash - 1) + "/logs".
ELSE
	cLogDir = "logs".

OS-CREATE-DIR VALUE(cLogDir).
cLog = cLogDir + "/04_loaduser_put.log".

OUTPUT TO VALUE(cLog) APPEND.
PUT UNFORMATTED "=== INICIO loaduser.p ===" SKIP.
PUT UNFORMATTED "FILE: " cFile SKIP.

SESSION:APPL-ALERT-BOXES = NO.

RUN prodict/load_d.p ("_User", INPUT cFile).

PUT UNFORMATTED "=== FIM loaduser.p ===" SKIP.
OUTPUT CLOSE.

QUIT.