#ifndef SORTED_H_
#define SORTED_H_
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include <string>
#include<fstream>
#include<sstream>
#include"Pipe.h"
#include"BigQ.h"
#include"Heap.h"
#include"GenericDBfile.h"

struct SortInfo
{
	OrderMaker *myOrder;
	int runLength;
};



class Sorted: public GenericDBfile {

private:
	Pipe *inPipe;
	Pipe *outPipe;
	BigQ *bigQ;

	bool bstFlag, foundRecBST;
	int recOfPage;
	Page page, *currentPage;
	File file, *addnewFile;
	
	Record *pointer;
	int pageNumber, currentPageNumber, firstRecord, runLength, noSortOrder, addpageNumber;
	int isWriting;/*0 is read, 1 is write*/
	char fileName[250];
	char metafileName[250];

	


public:
	OrderMaker sortOrderMaker;
	OrderMaker query, literalQuery;
	/**
	 * Constructor
	 */
	Sorted();
	
	void MoveFirst();

	int Create(char *name, fType myType, void *startup);
    
	int Open(char *name);
    int Close();
	
	void Add(Record &addMe);

	int GetNext(Record &fetchMe);
	int GetNext(Record &fetchMe, CNF &applyMe, Record &literal);

	void Load(Schema &mySchema, char *loadMe);

	void MergeData();
	int CreateSortorder(CNF &ts, Record &t);
	void AddRecord(Record&);

	virtual ~Sorted();
};

#endif /*SORTED_H_*/
