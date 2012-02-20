#include "RelOp.h"
void* DoSelectFileRun(void*);

//SelectFile::SelectFile(){

//}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
cout<<"Inside Select File operator"<<endl;
	in = &inFile;
	out = &outPipe;
	lit = &literal;
	selectionOp = selOp;

	int ret = pthread_create(&sfthread, NULL, DoSelectFileRun, (void*)this);
	if(ret!=0){ cerr << "Error in pthread create SF" ; return;}

}

void SelectFile::WaitUntilDone(){
	pthread_join(sfthread, NULL);
}
void SelectFile::Use_n_Pages (int runlen) {
	numpgs = runlen;
}
void* DoSelectFileRun(void* arg){
	SelectFile* sf = (SelectFile*) arg;
	int count = 0;
	Record fetchMe;
	//sf->in->MoveFirst();
	//int count = 0;
	while(sf->in->GetNext(fetchMe,sf->selectionOp,(Record&)*(sf->lit))){
		//cout<<"Selecting a record "<<count;	
			sf->out->Insert(&fetchMe);
		
		count++;
	}
	cout<<"SelectFile: Pushed Record   Count = "<<count<<endl;
	//sf->in->Close();
	sf->out->ShutDown();
	return NULL;
}

//SelectFile::~SelectFile()
//{
//}
