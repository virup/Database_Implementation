#include "DBFile.h"
#include <string>
#include <iostream>

using namespace std;

Heap::Heap()  //constructor
{
	//initialize
	pageNo = 0;
	curPageNo = 0;
	firstRec = 0;
	page.EmptyItOut();
	curRecord = new Record;
	
}


void Heap::MoveFirst() // move to the first record of the first page.
{
	curPageNo = 0;
	file.GetPage(&page,0);
	page.GetFirst(curRecord);
}
	
	
void Heap::Add(Record & addMe)  // Add records
{
	Page tempPage;
	tempPage.EmptyItOut();
	file.GetPage(&tempPage,pageNo);

	if(tempPage.Append(&addMe) != 1) /* if no space in the page then write it to disk*/
	{
		pageNo++;
		Page addingPage;
		addingPage.EmptyItOut();
		file.AddPage(&addingPage,pageNo);
		file.GetPage(&tempPage, pageNo);
		tempPage.Append(&addMe);
	}

	file.AddPage(&tempPage, pageNo);
	file.GetPage(&tempPage, pageNo);
}


int Heap::Create(char *name, fType myType, void *startUp) // create the file
{
	char metafileName[1024];
	strcpy(fileName, name);
	strcpy(metafileName, name);
	strcat(metafileName, ".meta");
	ofstream metaFile;
	metaFile.open(metafileName);
	metaFile << "Heap"<< endl;

	
	strcpy(fileName, name);
	//strcpy(metaFile, fileName);
	//strcat(metaFile,".head"); // open a header file. Not writing anything in this case.
	file.Open(0,name);
	Page p;
	file.AddPage(&p,0);
	pageNo = file.GetLength() - 2;
	file.Close();

	//this.myType = myType;
	if(Open(fileName))
		return 1;
	else 
		return 0;
}
		
int Heap::Open(char * name)	// open the file.
{
	file.Open(1,name);
	pageNo = file.GetLength() - 2;
	if(pageNo < 0) return 0;
	MoveFirst();
	return 1;
}

int Heap::Close() // close the file
{
	if(file.Close())
		return 1;
	else
		return 0;
}

int Heap::GetNext(Record &fetchMe)  // fetch next record.
{


	if(firstRec == 0)
	{
		//fetchMe.Copy(page.myRecs->Current(0));
		//This is the correction done from the first project submission
		fetchMe.Copy(curRecord);
		firstRec = 1;
		//cout<< endl<< "Made the first copy";
		return 1;

		
	}
	if(page.myRecs->RightLength() == 1)
	{
		curPageNo++;
		if(curPageNo > pageNo) return 0;
		file.GetPage(&page,curPageNo);
		page.myRecs->MoveToStart();
	}
	else if(firstRec != 1)
	{
		page.myRecs->Advance();
	}
	firstRec = 2;
	fetchMe.Copy(page.myRecs->Current(0));
	return 1;
}

int Heap::GetNext(Record &fetchMe, CNF &applyMe, Record &literal) // fetch next record baed on comparision.
{

	ComparisonEngine c;
	int flag = 0;
	while(1)
	{
		if(GetNext(fetchMe) == 0)
		{
			flag = 0; 
			break;
		}
		else
		{
			if(c.Compare(&fetchMe, &literal, &applyMe))
			{
				flag = 1;
				break;
			}
			else
				flag = 0;
		}
	}
	return flag;

}

void Heap::Load(Schema &mySchema, char * loadMe) // bulk load
{

	FILE *fileTable = fopen(loadMe , "r");
	Record tempRec;
	Page tempPage;
	file.GetPage(&tempPage,pageNo);
	int count = 0;

	while(1)
	{
		if(tempRec.SuckNextRecord(&mySchema, fileTable) == 1)
		{
			if(tempPage.Append(&tempRec) == 0)
			{
				file.AddPage(&tempPage, pageNo);
				pageNo++;
				Page *bufferPage = new Page();
				file.AddPage(bufferPage, pageNo);
				file.GetPage(&tempPage, pageNo);
				tempPage.Append(&tempRec);
			}
		}
		else
		{
			file.AddPage(&tempPage,pageNo);
			break;
		}
		if(count++ % 1000) cout<<".";		
	}

	MoveFirst();
}


void Heap::LoadfromPipe(Pipe *pipe, int c) {
	Record temp;
	//Page *newPage= new Page();
	Page newPage;
	file.GetPage(&newPage, pageNo);
	int i=0, cnt = 0;
	while (1) {
		if (pipe->Remove(&temp)==1) {
			//cout<<"Record added"<<++cnt<<endl;
			if ((newPage.Append(&temp)==0)) {
				//Write the page to disk and add a new page
				file.AddPage(&newPage, pageNo++);
				Page *buf = new Page();
				file.AddPage(buf, pageNo);
				file.GetPage(&newPage, pageNo);
				newPage.Append(&temp);
				delete buf;
			}
		} else {
			
			//Write the page to disk
			file.AddPage(&newPage, pageNo);
			break;
		}
		i++;

	}
	cout<<"\n PAGE NUMERN "<<c<<"  " << pageNo;
	//delete newPage;
	MoveFirst();
}



Heap::~Heap()
{		
}
	
