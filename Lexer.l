/***********************************************************************
 * SECTION 1
 ***********************************************************************/
/* The code in %{ %} is included as it is in lex.yy.c file
 * it has C global variables, prototypes, and comments 
 */


%{

#include <string.h> // For strdup()
#include <stdlib.h> // For malloc()
#include "ParseTree.h"
#include "y.tab.h"

int lineno = 1;
void yyerror(char*s);

static YY_BUFFER_STATE yy_buf_state;
	void init_lexical_parser_norm (char *src) { yy_buf_state = yyfunc_scan_string (src); }

void close_lexical_parser_norm () { yyfunc_delete_buffer (yy_buf_state); }

%}


/******************************************************************************
 * SECTION 2
 ******************************************************************************/
/* This is the DEFINITION section which contains substitutions, code, and
 * start stats; will be copied into lex.yy.c
 */

/******************************************************************************
 * SECTION 3
 ******************************************************************************/
/* This is the RULES section which defines how to "scan" and what action
 * to take for each token
 */

%%
"CREATE"		return(CREATE);

"TABLE"		return(TABLE);

"INTEGER"		return(INTEGER_TYPE);

"DOUBLE"		return(DOUBLE_TYPE);

"STRING"		return(STRING_TYPE);

"HEAP"		return(HEAP);

"SORTED"		return(SORTED);

"ON"		return(ON);

"INSERT"		return(INSERT);

"INTO"		return(INTO);

"DROP"		return(DROP);

"SET"		return(SET);

"OUTPUT"		return(OUTPUT);

"STDOUT"		return(STDOUT);

"NONE"		return(NONE);

"UPDATE"		return(UPDATE);

"STATISTICS"		return(STATISTICS);

"FOR"		return(FOR);

"for"		return(FOR);

"quit"		return(QUIT);

"QUIT"		return(QUIT);

"EXIT"		return(QUIT);

"exit"		return(QUIT);

";"		return(';');

"SELECT"		return(SELECT);

"FROM"			return(FROM);

"WHERE"			return(WHERE);

"SUM"			return(SUM);

"AND"			return(AND);

"GROUP"			return(GROUP);

"DISTINCT"		return(DISTINCT);

"BY"			return(BY);

"OR"			return(OR);

"AS"			return(AS);

"("			return('(');

"<"                     return('<');

">"                     return('>');

"="                     return('=');

")"    	        	return(')');

"+"    	        	return('+');

"-"    	        	return('-');

"/"    	        	return('/');

"*"    	        	return('*');

","    	        	return(',');

-?[0-9]+ 	       {yylval.actualChars = strdup(yytext);
  			return(Int); 
		        }

-?[0-9]+\.[0-9]*       {yylval.actualChars = strdup(yytext); 
  			return(Float);
			}

[A-Za-z][A-Za-z0-9_-]* {yylval.actualChars = strdup(yytext);
  			return(Name);
			}     

[A-Za-z][A-Za-z0-9_-]*\.[A-Za-z][A-Za-z0-9_-]* {yylval.actualChars = strdup(yytext);
  			return(Name);
			}     

\'[^'\n]*\'            {/* take care of ' in a string */
                        if (yytext[yyleng - 2] == '\\') {
                                yymore();
                        } else {
                                yylval.actualChars = strdup(yytext + 1);
                                yylval.actualChars[strlen(yylval.actualChars) - 1] = 0;
                                return(String);
                        }
                        }

\n                	lineno++;

[ \t]             	;

.                 	yyerror("LEX_ERROR: invalid character");

%%

void yyerror(char *s) {
  printf("%d: %s at %s\n", lineno, s, yytext);
}

int yywrap(void){
  return 1;
}


