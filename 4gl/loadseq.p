/* loadseq.p - load de sequências (batch-safe)
	 PARAM: diretório onde está seqvals.d
*/

DEFINE VARIABLE cDir    AS CHARACTER NO-UNDO.
DEFINE VARIABLE cLogDir AS CHARACTER NO-UNDO.
DEFINE VARIABLE cLog    AS CHARACTER NO-UNDO.

cDir = SESSION:PARAMETER.

IF cDir = ? OR cDir = "" THEN DO:
	OUTPUT TO VALUE("loadseq.error.log") APPEND.
	PUT UNFORMATTED "ERRO: loadseq.p sem parametro" SKIP.
	OUTPUT CLOSE.
	QUIT.
END.

cLogDir = cDir + "/logs".
OS-CREATE-DIR VALUE(cLogDir).
cLog = cLogDir + "/03_loadseq_put.log".

OUTPUT TO VALUE(cLog) APPEND.
PUT UNFORMATTED "=== INICIO loadseq.p ===" SKIP.
PUT UNFORMATTED "DIR: " cDir SKIP.

SESSION:APPL-ALERT-BOXES = NO.

RUN prodict/load_seq.p ("seqvals.d", cDir).

PUT UNFORMATTED "=== FIM loadseq.p ===" SKIP.
OUTPUT CLOSE.

QUIT.