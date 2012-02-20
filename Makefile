
CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

main: QueryPlan.o Statistics.o Sum.o Heap.o Sorted.o SelectFile.o SelectPipe.o Join.o Project.o GroupBy.o DuplicateRemoval.o WriteOut.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o main.o
	$(CC) -w -o main QueryPlan.o Statistics.o Heap.o Sorted.o SelectFile.o SelectPipe.o Join.o Project.o GroupBy.o DuplicateRemoval.o WriteOut.o Sum.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o main.o -lfl -lpthread

a4-1.out: Statistics.o Sum.o Heap.o Sorted.o SelectFile.o SelectPipe.o Join.o Project.o GroupBy.o DuplicateRemoval.o WriteOut.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o
	$(CC) -o a4-1.out Statistics.o Heap.o Sorted.o SelectFile.o SelectPipe.o Join.o Project.o GroupBy.o DuplicateRemoval.o WriteOut.o Sum.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o -lfl -lpthread

a3test.out: Sum.o Heap.o Sorted.o SelectFile.o SelectPipe.o Join.o Project.o GroupBy.o DuplicateRemoval.o WriteOut.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o
	$(CC) -o a3test.out Heap.o Sorted.o SelectFile.o SelectPipe.o Join.o Project.o GroupBy.o DuplicateRemoval.o WriteOut.o Sum.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a3-test.o -lfl -lpthread
	
a2-2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-2test.o
	$(CC) -o a2-2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-2test.o -lfl -lpthread
	
a2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-test.o
	$(CC) -o a2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-test.o -lfl -lpthread
	
a1test.out: Sorted.o BigQ.o Heap.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o  a1-test.o
	$(CC) -o a1test.out Sorted.o BigQ.o Heap.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a1-test.o -lfl -lpthread
	
test.o: test.cc
	$(CC) -g -w -c test.cc

a3test.o: a3test.cc
	$(CC) -g -c a3test.cc
	
a2-2test.o: a2-2test.cc
	$(CC) -g -c a2-2test.cc

a2-test.o: a2-test.cc
	$(CC) -g -c a2-test.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

Comparison.o: Comparison.cc
	$(CC) -g -w -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -w -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc 

main.o : main.cc
	$(CC) -g -w -c main.cc

Heap.o: Heap.cc
	$(CC) -g -c Heap.cc
Sorted.o: Sorted.cc
	$(CC) -g -w -c Sorted.cc
SelectFile.o: SelectFile.cc
	$(CC) -g -c SelectFile.cc
SelectPipe.o: SelectPipe.cc
	$(CC) -g -c SelectPipe.cc
Join.o: Join.cc
	$(CC) -g -w -c Join.cc
Project.o: Project.cc
	$(CC) -g -c Project.cc
GroupBy.o: GroupBy.cc
	$(CC) -g -c GroupBy.cc
DuplicateRemoval.o: DuplicateRemoval.cc
	$(CC) -g -c DuplicateRemoval.cc
WriteOut.o: WriteOut.cc
	$(CC) -g -c WriteOut.cc
Sum.o: Sum.cc
	$(CC) -g -c Sum.cc

QueryPlan.o: QueryPlan.cc
	$(CC) -g -w -c QueryPlan.cc

Statistics.o: Statistics.cc
	$(CC) -g -w -c Statistics.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c -w y.tab.c
		
yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c -w yyfunc.tab.c
	
lex.yy.o: Lexer.l
	lex Lexer.l
	gcc  -c -w lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c -w lex.yyfunc.c


clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
