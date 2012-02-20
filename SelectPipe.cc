#include "RelOp.h"
#include <iostream.h>



void *StartRun(void *);
SelectPipe::SelectPipe()
{
	noOfPages = 10;
}


void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal)
{
	in = &inPipe;
	out = &outPipe;
	oriRecord = &literal;
	selectionOp = &selOp;
	
	// Create the thread
	int ret = pthread_create(&thread, NULL, &StartRun, (void *)this);
	if(ret){ std::cout << "Error in pthread create SP" ; return;}
}

void SelectPipe::WaitUntilDone()
{
	pthread_join(thread, NULL);

}

void SelectPipe::Use_n_Pages (int runlen) {
	// Empty for now
}

void SelectPipe::ActualRun()
{
	ComparisonEngine compEngine;
	while(in->Remove(buffer))
	{
		if(compEngine.Compare(buffer, oriRecord, selectionOp))
			out->Insert(buffer);

	}
	out->ShutDown();
	//return NULL;
}


void *StartRun(void *temp)
{
	//SelectPipe *selectPipe= dynamic_cast<SelectPipe *> ((SelectPipe *)temp);
	SelectPipe *selectPipe= ((SelectPipe *)temp);
	selectPipe->ActualRun();
}

