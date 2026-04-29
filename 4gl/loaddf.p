/* loaddf.p - batch-safe
	 PARAM: caminho completo do arquivo .df a carregar
*/
DEFINE VARIABLE cDf AS CHARACTER NO-UNDO.

cDf = SESSION:PARAMETER.

IF cDf = ? OR cDf = "" THEN DO:
	QUIT.
END.

SESSION:APPL-ALERT-BOXES = NO.

RUN prodict/load_df (INPUT cDf).

QUIT.