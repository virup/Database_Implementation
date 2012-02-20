#include "RelOp.h"
void* DoProjectRun(void*);

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, 
	int numAttsInput, int numAttsOutput) { 

	in = &inPipe;
	out = &outPipe;
	inLength = numAttsInput;
	outLength = numAttsOutput;
	atts = keepMe;//TODO: keepme sorted


	int ret = pthread_create(&thread, NULL, DoProjectRun, (void*)this);
	if(ret!=0){ cerr << "Error in pthread create PO" ; return;}

}

void Project::WaitUntilDone(){
	pthread_join(thread, NULL);
}

void Project::Use_n_Pages (int runlen) {
	// Empty for now
}

void* DoProjectRun(void* arg){
	cout<<" do project ";
	Project* proj = (Project*) arg;
	int count = 0;
	Record fetchMe;
	while(proj->in->Remove(&fetchMe)){
		//cout<<" inside the project while ... "<< count;
		//cout<<". outlength = "<<proj->outLength<<" In length = " <<proj->inLength;
		
		fetchMe.Project(proj->atts,proj->outLength,proj->inLength);
		proj->out->Insert(&fetchMe);
		++count;
//sleep(1);
	}
	cout <<"::::PROJECT COUNT = "<< count << endl;
	//proj->in->ShutDown();
	proj->out->ShutDown();
	return NULL;
}
