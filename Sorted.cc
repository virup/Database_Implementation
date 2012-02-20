#include "iostream.h"
#include "string.h"
#include "Comparison.h"
#include "Sorted.h"
#include <stdlib.h>

using namespace std;

Sorted::Sorted() 
{
	
	currentPage= new Page();
	pointer = new Record();
	pageNumber=currentPageNumber=firstRecord=0;
	noSortOrder = 0;
	isWriting = 0;
	recOfPage=0;
	addpageNumber = 0;
	addnewFile = new File();
	foundRecBST=bstFlag=false;
}

int Sorted::Create(char *name, fType myType, void *startup) 
{
	strcpy(fileName, name);
	strcpy(metafileName, name);
	strcat(metafileName, ".meta");
	

	SortInfo *sortInfo = (SortInfo *)startup;

	file.Open(0, name);
	Page *addme= new Page();
	file.AddPage(addme, 0);
	pageNumber = file.GetLength()-2;
	file.Close();
	

	ofstream metaFile;
	metaFile.open(metafileName);
	metaFile << "Sorted"<< endl;
	metaFile << (sortInfo)->runLength<< endl;

	metaFile << ((sortInfo)->myOrder->numAtts)<<endl;
	for (int i = 0; i < (sortInfo)->myOrder->numAtts; i++) 
	{
		metaFile << ((sortInfo)->myOrder->whichAtts[i])<<endl;
		
		switch((sortInfo)->myOrder->whichTypes[i])
		{
		case Int: metaFile<<"Int\n"; break;
		case Double: metaFile<<"Double\n";break;
		default: metaFile<<"String\n";
		}
	}
	metaFile.close();
	return 1;
}

int Sorted::Open(char *name) 
{
	strcpy(fileName, name);
	strcpy(metafileName, name);
	strcat(metafileName, ".meta");
	ifstream metaFile(metafileName);
	string readLine;


	file.Open(1, name);
	pageNumber = file.GetLength()-2;
	if (pageNumber < 0)
		return 0;

	
	int loopCount = 1;
	if (metaFile.is_open()) 
	{
		while (!metaFile.eof()) 
		{
			int num;
			if (loopCount < 3) 
			{
				getline(metaFile, readLine);
				if (loopCount==2)
				{
					stringstream ss(readLine);
					ss >> num;
					runLength = num;
				}
			}
			if (loopCount==3)
			{
				getline(metaFile, readLine);
				stringstream ss(readLine);
				ss >> num;
				sortOrderMaker.numAtts = num;
			}
			else if (loopCount>3) 
			{
				int cnt = 0;
				while (!metaFile.eof())
				{
					getline(metaFile, readLine);
					stringstream ss(readLine);
					ss >> num;
					sortOrderMaker.whichAtts[cnt] = num;
					getline(metaFile, readLine);
					if (readLine.compare("Int")==0)
						sortOrderMaker.whichTypes[cnt++] = Int;
					else if (!readLine.compare("Double"))
						sortOrderMaker.whichTypes[cnt++] = Double;
				    else if (!readLine.compare("String"))
						sortOrderMaker.whichTypes[cnt++] = String; 
					else 
					{

					}
				}
			}
			loopCount++;
		}
		metaFile.close();
	}
	else 
		return 0;
	return 1;
}

int Sorted::Close()
{
	if (isWriting==1) {
		isWriting = 0;
		MergeData();
	}
	if (file.Close())
		return 0;
	else
		return 1;
}

void Sorted::MoveFirst()
{
	//Get the first page

	if (isWriting==1) {
		isWriting = 0;
		MergeData();
	}
	currentPageNumber=0;

	file.GetPage(currentPage, 0);
	//Move to the first record
	currentPage->myRecs->MoveToStart();

	pointer=currentPage->myRecs->Current(0);

}
void Sorted::AddRecord(Record &rec) {
	if ((page.Append(&rec)==0)) {
		//Write the page to disk and add a new page
		addnewFile->AddPage(&page, addpageNumber++);
		Page *buf = new Page();
		buf->EmptyItOut();
		addnewFile->AddPage(buf, addpageNumber);
		addnewFile->GetPage(&page, addpageNumber);
		page.Append(&rec);
	}
}
void Sorted::MergeData() 
{
	int pipeover=0, fileover = 0;
	inPipe->ShutDown();
	file.Close();

	char renameStr[256];
	strcpy(renameStr, "mv ");
	strcat(renameStr, fileName);
	strcat(renameStr, " part1.bin");
	system(renameStr);

	addnewFile->Open(0, fileName);
	Record pipeRec;
	Record fileRec;
	Heap heapFile;
	heapFile.Open("part1.bin");
	heapFile.MoveFirst();
	outPipe->Remove(&pipeRec);
	if (heapFile.page.numRecs==0) 
	{
		fileover = 1;
	}
	if (fileover!=1) 
	{
		heapFile.GetNext(fileRec);
		while (1) {

			ComparisonEngine *ceng= new ComparisonEngine();
			int t = ceng->Compare(&pipeRec, &fileRec, &sortOrderMaker);
			if (t<0) {

				AddRecord(pipeRec);
				if (outPipe->Remove(&pipeRec)!=1) {
					pipeover = 1;
					break;
				}
			} else {
				AddRecord(fileRec);
				if (heapFile.GetNext(fileRec)!=1) {
					fileover = 1;
					break;
				}

			}
		}
	}
	if (pipeover==1) {
		AddRecord(fileRec);
		while (heapFile.GetNext(fileRec)==1) {
			AddRecord(fileRec);
		}
	}
	int ctr =0;
	if (fileover==1) {
		AddRecord(pipeRec);
		while (outPipe->Remove(&pipeRec)==1) {
			AddRecord(pipeRec);
		}

	}
	heapFile.Close();

	addnewFile->AddPage(&page, addpageNumber++);
	addpageNumber = 0;
	addnewFile->Close();

	if (remove("part1.bin") != 0)
		perror("Error deleting file");
	file.Open(1, fileName);
	pageNumber = file.GetLength()-2;
	firstRecord = 0;
}

int Sorted::GetNext(Record &fetchMe) {
	//If first record of first page then just return that
	if (isWriting==1) {
		isWriting = 0;
		MergeData();

	}
	if (firstRecord ==0) {
		fetchMe.Copy(currentPage->myRecs->Current(0));
		firstRecord =1;
		return 1;
	}
	//Last record
	else if (currentPage->myRecs->RightLength()==1) {
		currentPageNumber++;
		//All the pages traversed
		if (currentPageNumber>pageNumber) {
			return 0;
		}
		file.GetPage(currentPage, currentPageNumber);
		currentPage->myRecs->MoveToStart();
	}
	//For all the other records
	else
		currentPage->myRecs->Advance();

	fetchMe.Copy(currentPage->myRecs->Current(0));
	return 1;

}

int Sorted::CreateSortorder(CNF &incomming, Record &t) {
	CNF sort_pred= incomming;
	int arr[10];
	int k=0;
	for (int i=0; i<sort_pred.numAnds; i++) {
		if (sort_pred.orLens[i]==1) {
			if (sort_pred.orList[i][0].op == Equals) {
				k++;
				arr[i]= sort_pred.orList[i][0].whichAtt1;
			}
		}
	}
	OrderMaker query_make;
	int noAtts=0;
	int flag=0;

	for (int l=0; l<sortOrderMaker.numAtts; l++) {
		flag=0;
		for (int j=0; j<k; j++) {
			if (sortOrderMaker.whichAtts[l]==arr[j]) {
				query_make.whichAtts[noAtts]=arr[j];
				query_make.whichTypes[noAtts]= sortOrderMaker.whichTypes[l];
				noAtts++;
				flag=1;
				break;
			}
		}
		if (flag==0)
			break;
	}
	query_make.numAtts=noAtts;
	query=query_make;
	if (query.numAtts == 0) {
		noSortOrder = 1;
		return 0;
	}
	noAtts = 0;
	for (int l=0; l<query.numAtts; l++) {
		flag=0;
		for (int j=0; j<k; j++) {
			if (query.whichAtts[l]==arr[j]) {
				literalQuery.whichAtts[noAtts]=j;
				literalQuery.whichTypes[noAtts]= sortOrderMaker.whichTypes[l];
				noAtts++;
				flag=1;
				break;
			}
		}
		if (flag==0)
			break;
	}
	literalQuery.numAtts=noAtts;
	return query.numAtts;
}

int Sorted::GetNext(Record &fetchMe, CNF &applyMe, Record &literal) 
{
	int temp,pageNo, first=0, mid=0, foundrecord=0;;
	int end = file.GetLength()-2;


	ComparisonEngine *ceng= new ComparisonEngine();
	

	if (noSortOrder == 1)
		goto noMatch;

	if (!bstFlag)
	{
		bstFlag = true;
		if (CreateSortorder(applyMe, literal)==0)
		{
			noSortOrder = 1;
			recOfPage = 0;
			MoveFirst();
			bstFlag=true;
			goto noMatch;
		}

		while (first <= end) 
		{
			mid = (first + end ) / 2;
			file.GetPage(currentPage, mid);
			currentPageNumber=mid;

			currentPage->myRecs->MoveToStart();
			temp=ceng->Compare(currentPage->myRecs->Current(0), &query,
				&literal, &literalQuery);
			if (temp ==0) 
			{ //search for first rec, back track till u get the record
				foundRecBST=true;
				foundrecord=1;
				fetchMe.Copy((currentPage->myRecs->Current(0)));
				break;
			} 
			else if (temp < 0) 
			{ 
				int temp1=ceng->Compare(
					currentPage->myRecs->Current(currentPage->numRecs-1),
					&query, &literal, &literalQuery);

				if (temp1 >= 0) 
				{
					foundrecord=2;
					foundRecBST=true;
					break;
				}
				
				first= mid+1;
			} 
			else 
				end = mid - 1;
		}

		switch (foundrecord) {
		case 1: //job is to find the first record, matching, back track
			pageNo=mid-1;
			recOfPage=0;
			while (true) {
				if (pageNo!=-1)
				{
					file.GetPage(currentPage, pageNo--);
					currentPage->myRecs->MoveToStart();
					for (int jk = currentPage->numRecs-1; jk >=0; jk--) 
					{
						temp=ceng->Compare(currentPage->myRecs->Current(jk),
							&query, &literal, &literalQuery);
						if (temp!=0) 
						{
							if (jk ==currentPage->numRecs-1) 
							{
								file.GetPage(currentPage, mid);
								currentPageNumber=mid;
								currentPage->myRecs->MoveToStart();
							}
							goto f;
						}
						recOfPage=jk;
						fetchMe.Copy((currentPage->myRecs->Current(jk)));

					}
					mid--;
				} 
				else 
				{
					recOfPage=0;
					break;
				}
			}
f:
			goto firstTime;
			break;
		case 2: // job is to find the matching record in this page, use binary search
			currentPage->myRecs->MoveToStart();
			for (int jk = currentPage->numRecs-1; jk >=0; jk--) {
				temp=ceng->Compare(currentPage->myRecs->Current(jk), &query,
					&literal, &literalQuery);
				if (temp<0)
				{
					fetchMe.Copy(currentPage->myRecs->Current(jk+1));
					foundRecBST=true;
					recOfPage=jk+1;
					goto firstTime;
				}

			}
			break;
		}
		if (!foundRecBST) 
		{
			bstFlag=false;
			return 0;
		}
		bstFlag=true;
	} 
	else if (foundRecBST==true) 
	{
		recOfPage++;
		if (recOfPage > currentPage->numRecs-1) 
		{
			if (currentPageNumber== file.GetLength()-2) 
			{
				bstFlag=false;
				return 0;
			}

			file.GetPage(currentPage, ++currentPageNumber);
			currentPage->myRecs->MoveToStart();
			recOfPage=0;
		}
		fetchMe.Copy((currentPage->myRecs->Current(recOfPage)));

		if (ceng->Compare(&fetchMe, &query, &literal, &literalQuery) == 0) 
		{
			goto firstTime;
		}
		else
		{
			bstFlag=false;
			return 0;
		}
	} 
	else if (foundRecBST == false)
	{
		bstFlag=false;
		return 0;
	}

	if (noSortOrder == 166766) 
	{
		ComparisonEngine comp;

		while (1)
		{
			recOfPage++;
			if (recOfPage > currentPage->numRecs-1)
			{
				if (currentPageNumber== file.GetLength()-2)
				
				{
					bstFlag=false;
					return 0;
				}
				file.GetPage(currentPage, ++currentPageNumber);
				currentPage->myRecs->MoveToStart();
				recOfPage=0;
			}
			fetchMe.Copy((currentPage->myRecs->Current(recOfPage)));

			//No records left
			temp=ceng->Compare(&fetchMe, &query, &literal, &literalQuery);
			if (temp == 0) {
	firstTime:
				if (comp.Compare(&fetchMe, &literal, &applyMe))//problem
				{
					bstFlag=true;
					return 1;
				}
			} 
			else 
			{
				bstFlag=false;
				return 0;
			}

		}
	}

	if (noSortOrder == 1) 
	{
	noMatch: ComparisonEngine comp;
		while (1) 
		{
			int n = GetNext(fetchMe);
			// No records left
			if (n==0) 
				return 0;			 
			else 
				if (comp.Compare(&fetchMe, &literal, &applyMe))
					return 1;
		}
	}

}
void Sorted::Add(Record &addMe) 
{

	if (isWriting==0) 
	{

		isWriting = 1;
		inPipe = new Pipe(100);
		outPipe = new Pipe(100);
		bigQ = new BigQ(*inPipe, *outPipe, sortOrderMaker, runLength);
		//cout<<" made BigQ "<<endl;
	}//cout<<" Inside isWriting : Add()";
	inPipe->Insert(&addMe);
}

void Sorted::Load(Schema &mySchema, char *loadMe)
{
	if (isWriting==0) {
		isWriting = 1;
		inPipe = new Pipe(100);
		outPipe = new Pipe(100);
		bigQ = new BigQ(*inPipe, *outPipe, sortOrderMaker, runLength);
	}
	FILE *tableFile = fopen(loadMe, "r");
	while (1) {
		Record temp;
		if (temp.SuckNextRecord(&mySchema, tableFile) == 1) {
			inPipe->Insert(&temp);
		}
		else 
			break;	
	}

}

Sorted::~Sorted() 
{
}
