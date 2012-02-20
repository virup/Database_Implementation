#include "RelOp.h"
#include "Comparison.h"




void* DoGroupByRun(void*);


void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
	in = &inPipe;
	out = &outPipe;
	orderMaker = &groupAtts;
	func = &computeMe;
	numPages = 1;

	int ret = pthread_create(&thread, NULL, DoGroupByRun, (void*)this);
	if(ret!=0){ cerr << "Error in pthread create GB" ; return;}
}

void GroupBy::WaitUntilDone(){
	pthread_join(thread, NULL);
}
void GroupBy::Use_n_Pages (int runlen) {
	numPages = runlen;
}

void* DoGroupByRun(void* arg){
	GroupBy* gb = (GroupBy*) arg;
	cout<<" inside GB"<<endl;
	Pipe tmpOutPipe(100);
	//OrderMaker sortorder(dr->schema);
	BigQ bigq(*(gb->in), tmpOutPipe, *(gb->orderMaker),100);
	
	Record tmpCurRec, tmpBufRec;
	// Get the first record to have one in the buffer
	tmpOutPipe.Remove(&tmpBufRec);
	
	cout<<"inside GB 2"<<endl;
	ComparisonEngine compEngine;
	int iTotal = 0; 
	int iVal = 0;
	double dTotal = 0.0;
	double dVal = 0.0;
	bool intFlag = false;
	Record result; // Record to store the result of the groupby
	bool residue = false;
	
	int group_count = 0;
	
	// Apply function over the first record
	if(gb->func->Apply(tmpBufRec, iVal, dVal)==Int){
		iTotal += iVal;
		intFlag = true;
	}
	else{
		dTotal += dVal;
		intFlag = false;
	}
//cout<<" Going into loop"<<endl;
int count = 0;
	while(tmpOutPipe.Remove(&tmpCurRec))
	{
		
		cout<<"--"<<count++<<endl;
		if(compEngine.Compare(&tmpCurRec, &tmpBufRec, gb->orderMaker) == 0)
		{
			//cout<<" Inside compare";
			tmpBufRec.Copy(&tmpCurRec);
			// This is  a duplicate record------------
			// Apply the function and store the value  in itotal or dtotal
	//cout<<" In GB Compare "<<endl;
			iVal = 0;
			dVal = 0.0;
			if(gb->func->Apply(tmpCurRec, iVal, dVal)==Int)
			{
				iTotal += iVal;
				intFlag = true;
			}
			else
			{
				dTotal += dVal;
				intFlag = false;
			}
			
		}
		else

		{
			//cout<<" Grouping ! ";
			tmpBufRec.Copy(&tmpCurRec);	
			//cout<< " Grouping ..."<<endl;
			// One Group ends here. Process it and stuff into the output pipe
			// Somehow send the collected data into the pipe
			if(intFlag)
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
				result.SetBits(bits);
				
				gb->out->Insert(&result);
				delete [] recSpace;
			}
			else
			{
				cout<<" Double";
				char *recSpace = new (std::nothrow) char[PAGE_SIZE];
				int n = 3;
				int currentPosInRec = sizeof (int) * (n + 1);
				// this would be in the for loop
				((int *) recSpace)[0 + 1] = currentPosInRec;
				while (currentPosInRec % sizeof(double) != 0) 
				{
					currentPosInRec += sizeof (int);
					((int *) recSpace)[0 + 1] = currentPosInRec;
				}
				*((double *) &(recSpace[currentPosInRec])) = dTotal;
				currentPosInRec += sizeof (double);
				// the last thing is to set up the pointer to just past the end of the reocrd
				((int *) recSpace)[0] = currentPosInRec;
				char* bits = new (std::nothrow) char[currentPosInRec];
				memcpy (bits, recSpace, currentPosInRec);
				result.SetBits(bits);
				
				gb->out->Insert(&result);
				delete [] recSpace;
			}
			// Reset Group
			// Apply the function to the first  
			iVal = 0;
			dVal = 0.0;
			iTotal = 0;
			dTotal = 0.0;
			if(gb->func->Apply(tmpCurRec, iVal, dVal)==Int){
				iTotal += iVal;
			}
			else
			{
				dTotal += dVal;
			}
			
			// Read the next record from the outputPipe of the bigQ
		//End Group processing
		
		
		//cout << "."<<++group_count;;
		} 
		cout<<" none";
	}
	cout << " groupCount=" << group_count << "\n";
	tmpOutPipe.ShutDown();
	
	residue  = true;
	if(residue){
		cout<<" inside residue .. ";
		// Process the records that are already read....
		//  stuff the last total into the pipe, thats it.
		if(intFlag)
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
			result.SetBits(bits);
			
			gb->out->Insert(&result);
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
			result.SetBits(bits);
			
			gb->out->Insert(&result);
			delete [] recSpace;		
		}
	}
	gb->out->ShutDown();

}

