#include "RelOp.h"
#include "Comparison.h"

void* DoDuplicateRemovalRun(void* arg);

/*DuplicateRemoval::DuplicateRemoval(){
}*/

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
	in = &inPipe;
	out = &outPipe;
	schema = &mySchema;
	numPages = 100;

	int ret = pthread_create(&thread, NULL, DoDuplicateRemovalRun, (void*)this);
	if(ret!=0){ cerr << "Error in pthread create DR" ; return;}
}

void DuplicateRemoval::WaitUntilDone(){
	pthread_join(thread, NULL);
}
void DuplicateRemoval::Use_n_Pages (int runlen) {
	numPages = runlen;
}

void* DoDuplicateRemovalRun(void* arg){
	DuplicateRemoval* dr = (DuplicateRemoval*) arg;
	
	Pipe tmpOutPipe(dr->numPages);
	OrderMaker sortorder(dr->schema);

	//BigQ bigq( dr->in, tmpOutPipe, sortorder,(int)dr->numPages);
	BigQ bigq( *(dr->in), tmpOutPipe, sortorder,dr->numPages);
	
	Record curRec, bufRec;
	
	// Get the first record to have one in the buffer
	tmpOutPipe.Remove(&bufRec);
	
	//cout <<" Num bits " << bufRec.GetNumAtts()<<"\n"; 
	curRec.Copy(&bufRec); // preserve the inserted record
	dr->out->Insert(&bufRec);
	bufRec.Copy(&curRec);
	
	ComparisonEngine compEngine;
	while(tmpOutPipe.Remove(&curRec)){

		if(compEngine.Compare(&curRec, &bufRec, &sortorder) != 0){
			bufRec.Copy(&curRec); // Send this record to the buffer
			dr->out->Insert(&curRec);

		}
		else {
			// this is a duplicate record
			// Keep the same buffer and keep going
		}
	}
	tmpOutPipe.ShutDown();
	dr->out->ShutDown();
	
	return NULL;
}
