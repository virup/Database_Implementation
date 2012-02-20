#include "BigQ.h"
void *StartSortProcess(void *temp);

BigQ::BigQ() {
	inPipe= new Pipe(100);
	outPipe = new Pipe(100);
	runLength=0;
}

BigQ::~BigQ() {
}
void BigQ::done(){
	pthread_join (thread, NULL);
}

BigQ::BigQ(Pipe &inputPipe, Pipe &outputPipe, OrderMaker &sortOrder, int Length) 
{
	//cout<<"BigQ start"<<endl;
	runLength=Length;
	order=&sortOrder;
	inPipe=&inputPipe;
	outPipe=&outputPipe;
	pthread_create(&thread, NULL, &StartSortProcess, (void *)this);

}
void BigQ::sortProcess() {

	char tempFileName[1024];
	//int *count;
	//bool *isLastRun; 
	//int *pageCount;
	//int *donePage;
	int i=0;
	int pageNo=0;
	int noOfRuns = 1;
	int lastRunNoPage=0;
	int totalRecords=0;
	File tempFile;
	Page tempPage, *myPageNew;

	tmpnam(tempFileName);
	//cout<<" Sort Process"<<endl;


	//cout<<" temp name = " << tempFileName;

	tempFile.Open(0, tempFileName);	
	tempFile.AddPage(&tempPage, 0);
	tempFile.Close();
	tempFile.Open(1, tempFileName);
	pageNo = tempFile.GetLength()-2;



	tempFile.GetPage(&tempPage, 0);
	tempPage.myRecs->MoveToStart();

	long maxSize = PAGE_SIZE * runLength - (runLength * 150);
	int curSize = 0;

	bool flag = false;
	int loopCount=0;

	SortRecordClass *sortedRecord;
	Record *tempRecord;
	
	while (!flag && (curSize<=maxSize)) 
	{

		curSize=0;
		priority_queue< SortRecordClass* , vector < SortRecordClass* >, SortRecordClass> tempPriorityQueue;
		while (true) 
		{
			sortedRecord = new SortRecordClass();
			tempRecord= new Record();
		//	cout<<"BigQ: Removing"<<endl;
			if (inPipe->Remove(tempRecord)==0) 
			{
				flag=true;
				break;
			}
			//cout<<"BigQ: Removed"<<endl;
			totalRecords++;
			sortedRecord->record=(tempRecord);
			sortedRecord->orderMaker=order;
			tempPriorityQueue.push(sortedRecord);
			char *buffer = sortedRecord->record->GetBits();
			curSize = curSize + ((int *)buffer)[0];

		}

		Page *buf;
		//write to page
		while(!tempPriorityQueue.empty())
		{
			if ((tempPage.Append((tempPriorityQueue.top())->record)==0)) 
			{
				tempFile.AddPage(&tempPage, pageNo++);
				buf = new Page();
				tempFile.AddPage(buf, pageNo);
				tempFile.GetPage(&tempPage, pageNo);
				tempPage.Append((tempPriorityQueue.top())->record);
				delete buf;
			}
			tempPriorityQueue.pop();
		}
		loopCount++;

		tempFile.AddPage(&tempPage, pageNo);
		pageNo++;
		if( !flag) // Flag == 0: this means that the pipe is not yet finished, more records to be read.
		{
			// so we have to write more pages into the file.
			buf = new Page();
			tempFile.AddPage(buf, pageNo);
			tempFile.GetPage(&tempPage,pageNo);
			noOfRuns++;
			delete buf;
		}
	}

	lastRunNoPage = pageNo - ((runLength) * (loopCount-1));
	noOfRuns = (int)ceil((tempFile.GetLength()-1)/(runLength*1.0)); // get the number of runs from the tempfile

	int count [noOfRuns];
	bool isLastRun [noOfRuns]; 
	int pageCount [noOfRuns];
	int donePage [noOfRuns];

	priority_queue< SortRecordClass* ,vector <SortRecordClass* >, SortRecordClass> pq;


	for(int i=0;i<noOfRuns;i++)
	{  
		myPageNew = new Page();
		isLastRun[i] = (runLength == 1)?true:false;

		pageCount[i] = runLength;
		donePage[i] = 1;
	
		// Read a new page from the tempFile
		tempFile.GetPage(myPageNew, i*runLength);
		count[i] = myPageNew->GetNumRecs();
		myPageNew->myRecs->MoveToStart();

		// Put all the records from that page into a priority queue pq
		while(myPageNew->myRecs->RightLength()) 
		{
			SortRecordClass *newSortRecord = new SortRecordClass();
			newSortRecord->record = myPageNew->myRecs->Current(0);
			newSortRecord->run = i;
			newSortRecord->orderMaker = order;
			pq.push(newSortRecord);
			myPageNew->myRecs->Advance();
		}
	}

	pageCount[noOfRuns-1]=lastRunNoPage;
	if(lastRunNoPage==1) 
		isLastRun[noOfRuns-1]= true;
	
	

	SortRecordClass *newSortRecord;
	

	
	Page *bufferPage;
	for(int iTemp = 0; iTemp<totalRecords; iTemp++)
	{
		if(!pq.empty())
		{
			//cout <<" BigQ: Inserting in output pipe"<<endl;
			outPipe->Insert(&(*(pq.top())->record));
			int run= (pq.top())->run;
			pq.pop();

			count[run]--;
			if(count[run] == 0 && isLastRun[run] == 0)
			{  
				bufferPage = new Page();
				int pageLoadNo = (runLength * run ) + donePage[run];
				if(iTemp==totalRecords-1)
					break;
				tempFile.GetPage(bufferPage,pageLoadNo);
				bufferPage->myRecs->MoveToStart();
				donePage[run]++;
				newSortRecord= new SortRecordClass[bufferPage->GetNumRecs()];
				int d=0;
				while (bufferPage->myRecs->RightLength()>=1) 
				{
					count[run]++;
					newSortRecord[d].record = (&(*(bufferPage->myRecs->Current(0))));
					newSortRecord[d].run = run;
					newSortRecord[d].orderMaker = order;
					pq.push(&newSortRecord[d]);
					d++;
					if(bufferPage->myRecs->RightLength()==1) 
						break;
					bufferPage->myRecs->Advance();
										
				}
				if ((pageCount[run]-donePage[run])==0) 
						isLastRun[run]=true;
				
			}
		}
	}
	
	

	tempFile.Close();
	outPipe->ShutDown();
		
	if (remove(tempFileName) != 0)
		perror("Error deleting bin file ");

}

void *StartSortProcess(void *temp) {

	//BigQ *bigQ= dynamic_cast<BigQ *> ((BigQ *)temp);
	BigQ *bigQ= (BigQ *)temp;
	bigQ->sortProcess();
}
