#include "RelOp.h"
void* DoSumRun(void*);


void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){

	in = &inPipe;
	out = &outPipe;
	func = &computeMe;

	int ret = pthread_create(&thread, NULL, DoSumRun, (void*)this);
	if(ret){ cerr << "Error in pthread create" ; return;}

}

void Sum::WaitUntilDone(){
	pthread_join(thread, NULL);
}

void Sum::Use_n_Pages (int runlen) {
	// Empty for now
}

void* DoSumRun(void* arg){
	Sum* sum = (Sum*) arg;

	Record toMe;
	//cout<< "before sum while "<<endl;
	int iTotal = 0;
	double dTotal = 0.0;
	
	double count = 0;
	while(sum->in->Remove(&toMe)){
		count++;
	//	if(count % 100000) cout <<" . " ;
	//cout<<"Sum ..."<<endl;
		int ival = 0;
		double dval = 0.0;
		if(sum->func->Apply(toMe, ival, dval)==Int){
			iTotal += ival;
		}
		else{
			//cout <<"dval: " << dval << endl;
			dTotal += dval;

		}
					
	}
	//cout << "Note: sizeof(int)" << sizeof(int) << endl;
	//cout << "Note: sizeof(double)" << sizeof(double) << endl;
	//cout << "Note: sizeof(float)" << sizeof(float) << endl;
	int i;
	int j = 2*sizeof(int);
	Record result;
	//cout<<"Sum count = "<<count;
	if(dTotal == 0.0) // this is an int
	{
		
		char *recSpace = new (std::nothrow) char[PAGE_SIZE];
		int n = 3;
		int currentPosInRec = sizeof (int) * (n + 1);
		
		// this would be in the for loop
		((int *) recSpace)[0 + 1] = currentPosInRec;
		
		*((int *) &(recSpace[currentPosInRec])) = iTotal;	
		currentPosInRec += sizeof (int);
		
		// the last thing is to set up the pointer to just past the end of the reocrd
		((int *) recSpace)[0] = currentPosInRec;
		
		char* bits = new (std::nothrow) char[currentPosInRec];
		memcpy (bits, recSpace, currentPosInRec);
		//result.SetBits(temp);
		result.SetBits(bits);
		delete [] recSpace;
	}
	else
	{
		char *recSpace = new (std::nothrow) char[PAGE_SIZE];
		int n = 3;
		int currentPosInRec = sizeof (int) * (n + 1);
		
		// this would be in the for loop
		((int *) recSpace)[0 + 1] = currentPosInRec;
		
		while (currentPosInRec % sizeof(double) != 0) {
			currentPosInRec += sizeof (int);
			((int *) recSpace)[0 + 1] = currentPosInRec;
		}
		
		*((double *) &(recSpace[currentPosInRec])) = dTotal;
		currentPosInRec += sizeof (double);
		
		// the last thing is to set up the pointer to just past the end of the reocrd
		((int *) recSpace)[0] = currentPosInRec;
		
		char* bits = new (std::nothrow) char[currentPosInRec];
		memcpy (bits, recSpace, currentPosInRec);
		
		//result.SetBits(temp);
		result.SetBits(bits);
		
		delete [] recSpace;
		
	}



	sum->out->Insert(&result);
	sum->out->ShutDown();
}
