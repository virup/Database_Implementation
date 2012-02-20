#include "RelOp.h"
#include "Schema.h"

void *StartRunJoin(void *); 
void *startCreateLeftFile(void *); void 
*startCreateRightFile(void *);
void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
	//joinRec = &literal;
	joinRec = new Record();
	joinRec->Copy(&literal);
	inL = &inPipeL;
	outL = new Pipe(100);
	inR = &inPipeR;
	outR = new Pipe(100);
	out = &outPipe;
	selectionOp = &selOp;
	leftRec = new Record();
	rightRec = new Record();
	noOfPages = 100;
	// Create the thread
	int ret = pthread_create(&thread, NULL, &StartRunJoin, (void *)this);
	if(ret!=0){ std::cout << "Error in pthread create JOIN" ; return;}
}
void Join::WaitUntilDone() {
	pthread_join(thread, NULL);
	delete outL;
	delete outR;
	cout<<"JOIN JOINED ! "<<endl;
}
void Join::Use_n_Pages (int n) {
	noOfPages = n;
}
void Join::ActualRun() {
	OrderMaker soL, soR;
	if(selectionOp->GetSortOrders(soL, soR)){

		BigQ bigQL(*inL, *outL,soL, noOfPages);
		BigQ bigQR(*inR,*outR,soR, noOfPages);
	
		ComparisonEngine compEngine;
		bool leftPipeFinished = false;
		bool rightPipeFinished = false;
		//cout<<"Inside same"<<endl;

		if(!outL->Remove(leftRec))
		{
			leftPipeFinished = true;
				cout<<"Returned from left pipe";
		}

		if(!outR->Remove(rightRec))
		{
			rightPipeFinished = true;
				cout<<"returned from right pipe";

		}





		int leftAttrNum = leftRec->GetNumAtts();
		int rightAttrNum = rightRec->GetNumAtts();

		int *attrList = new int[rightAttrNum+leftAttrNum];
		for(int i = 0; i < leftAttrNum; i++)
			attrList[i] = i;
		for(int i = 0; i < rightAttrNum; i++)
			attrList[i+ leftAttrNum ] = i;
		cout<<"starting join while =  "<< rightAttrNum+leftAttrNum << endl;
		int count = 0;
		int countL = 0;
		int countR = 0;

		/*
		if(compEngine.Compare(leftRec,&soL,rightRec, &soR) != 0 || compEngine.Compare(leftRec, rightRec,joinRec,selectionOp) != 1 )
		{
			while(!leftPipeFinished && !rightPipeFinished)
			{
				if(compEngine.Compare(leftRec,&soL,rightRec, &soR) == 0 && compEngine.Compare(leftRec, rightRec,joinRec,selectionOp) == 1 )
					break;
				else if(compEngine.Compare(leftRec,&soL,rightRec, &soR) < 0 || compEngine.Compare(leftRec, rightRec,joinRec,selectionOp) ==  0 ){
				
				if(!outL->Remove(leftRec)){
					cout<< "Ths left fin" << endl;
					leftPipeFinished = true;
				}
			}
			else if(compEngine.Compare(leftRec,&soL,rightRec, &soR) > 0  || compEngine.Compare(leftRec, rightRec,joinRec,selectionOp) == 0){
				countR++;
				if(!outR->Remove(rightRec)){
					cout << "The right fin"<< endl;;
					rightPipeFinished = true;
				}
			}
			}

		}
*/

		

		while(!leftPipeFinished && !rightPipeFinished)
		{
//cout <<" count = "<<count++;
			if(compEngine.Compare(leftRec,&soL,rightRec, &soR) == 0 /*&& compEngine.Compare(leftRec, rightRec, joinRec,selectionOp) == 1*/ )
			{

				/*buffer.MergeRecords(leftRec,rightRec,leftAttrNum, 
					rightAttrNum, attrList, leftAttrNum + rightAttrNum ,leftAttrNum);

				out->Insert(&buffer);
				if(!outL->Remove(leftRec)) {
					
					cout << "Left fin\n";
					leftPipeFinished = true;
					// clear the other pipe
					while(outR->Remove(rightRec)){}
				}
				countL++;
				if(!outR->Remove(rightRec)) {
					 
					 cout << "Right fin\n";
					rightPipeFinished = true;
					// clear other pipe
					while(outL->Remove(leftRec)){}
				}countR++;
				*/
				
			vector<Record *> leftV;
			vector<Record *> rightV;
			Record *tempLeft = new Record();
			Record *tempRight = new Record();
Record *templeftToGet = new Record();
Record *temprightToGet = new Record();
templeftToGet->Copy(leftRec);
temprightToGet->Copy(rightRec);



			tempLeft->Copy(leftRec);
			tempRight->Copy(rightRec);
			

		


	
			while(compEngine.Compare(leftRec, tempLeft, &soL)==0 /*&& compEngine.Compare(tempLeft,joinRec,selectionOp) == 1 */)
			{

				Record *n = new Record();
				n->Copy(tempLeft);	
				leftV.push_back(n);
				if(!outL->Remove(tempLeft))
				{
					leftPipeFinished = true;
					break;
				}
			}


int subcount = 0;	

			if(!leftPipeFinished)leftRec->Copy(tempLeft);
	

			while(compEngine.Compare(rightRec, tempRight, &soR)==0  )
			{
subcount++;
				Record *n = new Record();
				n->Copy(tempRight);	
				rightV.push_back(n);
				if(!outR->Remove(tempRight))
				{
					rightPipeFinished = true;
					break;
				}
				countR++;
			}
			if(!rightPipeFinished)rightRec->Copy(tempRight);
			
Schema s1("catalog","supplier");
Schema s2("catalog", "partsupp");


			Record t;
			Record *lr1 = new Record();
			Record *rr1= new Record();
			for(int ii=0; ii< leftV.size(); ii++) 
			{
				lr1->Copy(leftV.at(ii));
				for(int jj=0; jj< rightV.size(); jj++) 
				{
					rr1->Copy(rightV.at(jj));
					buffer.MergeRecords(leftV.at(ii),rightV.at(jj),leftAttrNum, rightAttrNum, attrList, leftAttrNum + rightAttrNum ,leftAttrNum);
					out->Insert(&buffer);
				}
			}

			delete lr1,rr1;
			leftV.clear();
			rightV.clear();
			

			delete tempLeft;
			delete tempRight;
			delete templeftToGet;
			delete temprightToGet;
	
				

			}
			else if(compEngine.Compare(leftRec,&soL,rightRec, &soR) < 0 ){
				
				if(!outL->Remove(leftRec)){
					//cout<< "Ths left fin" << endl;
					leftPipeFinished = true;
				}
			}
			else if(compEngine.Compare(leftRec,&soL,rightRec, &soR) > 0  ){
				
				if(!outR->Remove(rightRec)){
					//cout << "The right fin"<< endl;;
					rightPipeFinished = true;
				}
				countR++;
			}
		
		}

		
			while(!leftPipeFinished && !rightPipeFinished)
			{
				if(compEngine.Compare(leftRec,&soL,rightRec, &soR) == 0 && compEngine.Compare(leftRec, rightRec,joinRec,selectionOp) == 1 )
					break;
				else if(compEngine.Compare(leftRec,&soL,rightRec, &soR) < 0 ){
				
				if(!outL->Remove(leftRec)){
					//cout<< "Ths left fin" << endl;
					leftPipeFinished = true;
				}
			}
			else if(compEngine.Compare(leftRec,&soL,rightRec, &soR) > 0 ){
				countR++;
				if(!outR->Remove(rightRec)){
					//cout << "The right fin"<< endl;;
					rightPipeFinished = true;
				}
			}
			}
	cout<<"Join: Count R = "<<countR<<endl;
		outL->ShutDown();
		outR->ShutDown();
		out->ShutDown();
	}
	else
	{
		//cout<<" inside else"<<endl;
		pthread_t leftFileThread;
		pthread_t rightFileThread;

		File leftFile;
		File rightFile;
		leftFile.Open(0,"leftFile");
		rightFile.Open(0,"rightFile");

		int iCond = 1;
		int ret = pthread_create(&leftFileThread, NULL, 
			startCreateLeftFile, (void *)this);
		if(ret!=0){ std::cout << "Error in pthread create" ; return;}
		iCond = 2;
		ret = pthread_create(&rightFileThread, NULL, 
			startCreateRightFile, (void *)this);
		if(ret!=0){ std::cout << "Error in pthread create" ; return;}
		pthread_join(leftFileThread, NULL);
		pthread_join(rightFileThread,NULL);

		File leftHeapFile;
		File rightHeapFile;
		leftHeapFile.Open(1,"leftFile");
		rightHeapFile.Open(1,"rightFile");

		Page *pageRight = new Page[noOfPages];
		Page *pageLeft = new Page;
		int leftSize = leftHeapFile.GetLength()-1; //TODO: Check if this is working properly
		int rightSize = rightHeapFile.GetLength()-1;

		int previousAccessedL = 0;
		int previousAccessedR = 0;
		int pageAccessedL = 0;
		int pageAccessedR = 0;
		pageAccessedL += 1; // make it half of the size
		pageAccessedR += noOfPages; // make it half of the size

		if(pageAccessedL > leftSize) pageAccessedL = leftSize;
		if(pageAccessedR > rightSize) pageAccessedR = rightSize;

		Record leftRec, rightRec,mergeRec;
		do
		{
			for(int i = 0; i < (pageAccessedL-previousAccessedL);i++)
				leftHeapFile.GetPage((pageLeft + i), previousAccessedL 
				+ i);

			for(int i = 0; i < (pageAccessedR-previousAccessedR);i++)
				rightHeapFile.GetPage((pageRight + i), 
				previousAccessedR + i);

			for(int l = 0; l < pageLeft->GetNumRecs();l++)
			{
				//Record leftRec;
				leftRec.Copy(pageLeft->myRecs->Current(l));
				int leftAttrNum = leftRec.GetNumAtts();
				for(int r = 0; r < 
					(pageAccessedR-previousAccessedR);r++)
				{
					for(int inside = 0; pageRight[r].GetNumRecs(); 
						inside++)
					{

						rightRec.Copy(pageRight[r].myRecs->Current(inside));

						int rightAttrNum = rightRec.GetNumAtts();

						int *attrList = new int[leftAttrNum + 
							rightAttrNum];
						for(int i = 0; i < leftAttrNum+rightAttrNum; 
							i++)
							attrList[i] = i;

						mergeRec.MergeRecords(&leftRec,&rightRec,leftAttrNum, rightAttrNum, 
							attrList, leftAttrNum+rightAttrNum,leftAttrNum); // Check this line
						out->Insert(&mergeRec);
					}		

				}
			}
			previousAccessedL = pageAccessedL;
			previousAccessedR = pageAccessedR;
			pageAccessedL += 1;
			pageAccessedR += noOfPages; // check the size

			if(pageAccessedL > leftSize) pageAccessedL = leftSize;
			if(pageAccessedR > rightSize) pageAccessedR = rightSize;
			if(previousAccessedR == pageAccessedR){previousAccessedR = 
				0; pageAccessedR = 0;} // initialize the pointer of the right file to the beginning
		}while(pageAccessedL == previousAccessedL);
		leftHeapFile.Close();
		rightHeapFile.Close();
		remove("leftFile");
		remove("rightFile");
		out->ShutDown();
	}
}
// Create a file to store the data temporarily 
void Join::createFile(int iFile) {
	//cout <<"void Join::createFile(int iFile) \n";
	File *temp = new File;
	if(iFile)temp->Open(0,"leftFile");
	else temp->Open(0,"rightFile");
	Page p;
	int pageCount = 0;
	Record r;

	while(inL->Remove(&r))
	{
		if(!p.Append(&r))
		{
			temp->AddPage(&p,pageCount); // check page number
			p.EmptyItOut();
			pageCount++;
			p.Append(&r);
		}
	}

	if(!p.Append(&r))
	{
		temp->AddPage(&p,pageCount); // check page number
		p.EmptyItOut();
		p.Append(&r);
		temp->AddPage(&p,pageCount);
	}
	temp->Close();
}
void *StartRunJoin(void *temp) {
	//Join *join= dynamic_cast<Join *> ((Join *)temp);
	Join *join= (Join *)temp;
	join->ActualRun();
}
void *startCreateLeftFile(void *temp) {

	Join *join= dynamic_cast<Join *> ((Join *)temp);
	join->createFile(1);
}
void *startCreateRightFile(void *temp) {

	Join *join= dynamic_cast<Join *> ((Join *)temp);
	join->createFile(2);
}
