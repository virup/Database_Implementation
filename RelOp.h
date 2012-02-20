#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "ComparisonEngine.h"
#include "Comparison.h"


class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
	 pthread_t sfthread;

	public:
	 DBFile* in;
	 Pipe* out;
	 CNF selectionOp;
	 Record* lit;
	 int numpgs;
	
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	//~SelectFile();
};

class SelectPipe : public RelationalOp {
	
	private:
	pthread_t thread;
	Record *buffer, *oriRecord;
	int noOfPages; // Not done anything with it here
	Pipe *in, *out;
	CNF *selectionOp;
	public:

	SelectPipe();
	void ActualRun(); // The funtion to call from Run()
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Project : public RelationalOp { 
	private:
	 pthread_t thread;

	public:
	 Pipe* in;
	 Pipe* out;
	 int* atts;
	 int inLength;
	 int outLength;
	 
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Join : public RelationalOp { 

	private:
	pthread_t thread;
	Record *leftRec, *rightRec, *joinRec, buffer;
	int noOfPages; // Not done anything with it here
	Pipe *inL, *outL,*inR, *outR, *out;
	CNF *selectionOp;


	public:
	void ActualRun();
	void createFile(int i);
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) ;
	void Run (Pipe &inPipeL, Pipe &inPipeR, Schema &schemaL, Schema& schemaR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	//~Join();
/*
private:
	Pipe *leftPipe,*rightPipe,*outPipe;
	CNF selOp,sel;
	Record literalJoin;
	pthread_t threads;
	pthread_t thread_left,thread_right;
	int Max_page; int mutex;
	pthread_mutex_t mutex1;
	int leftPipeCount, rightPipeCount, leftFinish,rightFinish;
	int *attsToKeep;
	bool cFlag;
public:
	Join();
      void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
      void Thread_Select();
      void Thread_lrPipe(int);
      void WaitUntilDone();
      //int get();
      //void set(int);
      void Use_n_Pages(int);
      void joining(Page &, Page &);*/
};
class DuplicateRemoval : public RelationalOp {
	private:
	 pthread_t thread;
	 
	public:
	 Pipe* in;
	 Pipe* out;	
	 Schema* schema;
	 int numPages; // The Run Length of the internal BigQ

	//DuplicateRemoval();
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Sum : public RelationalOp {
	private:
	 pthread_t thread;

	public:
	 Pipe* in;
	 Pipe* out;
	 Function* func;

	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class GroupBy : public RelationalOp {
	
	pthread_t thread;
	public:	
	 Pipe* in;
	 Pipe* out;
	 Function* func;
	 OrderMaker* orderMaker;
	 int numPages;

	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone () ;
	void Use_n_Pages (int n);
	
};
class WriteOut : public RelationalOp {
	private:
	 pthread_t thread;
	
	public:
	 Pipe* in;
	 FILE* out;
	 Schema* schema;
	void Run (Pipe &inPipe, FILE* outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
#endif
