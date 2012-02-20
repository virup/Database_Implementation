#include "RelOp.h"
void* DoWriteOutRun(void*);

void WriteOut::Run (Pipe &inPipe, FILE* outFile, Schema &mySchema){
	in = &inPipe;
	out = outFile;
	schema = &mySchema;

	int ret = pthread_create(&thread, NULL, DoWriteOutRun, (void*)this);
	if(ret!=0){ cerr << "Error in pthread create WO" << ret; return;}
}

void WriteOut::WaitUntilDone (){
	pthread_join(thread, NULL);
}

void WriteOut::Use_n_Pages (int n) {
	// Empty
}

void* DoWriteOutRun(void* arg){
	//cout <<" Inside dowriteoutRun"<<endl;
	WriteOut* wo = (WriteOut*)arg;

	Record temp;
	while(wo->in->Remove(&temp)){
		//cout<<"inside while do write"<<endl;
		temp.Print2File(wo->schema,wo->out);
	}
	fclose(wo->out);
	return NULL;
}
