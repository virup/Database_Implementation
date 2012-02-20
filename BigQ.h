#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "queue.h" 
#include <stdio.h>
#include <math.h>
#include "ComparisonEngine.h"

using namespace std;
void *StartSortProcess(void *);


class BigQ {

	int runLength;
	Pipe *inPipe, *outPipe;
	pthread_t thread;
	OrderMaker *order;

public:

	BigQ();
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	void sortProcess();
	void done();
	~BigQ ();
};



// Class to store Records

class SortRecordClass
{
	friend class BigQ;
	

	Record *record;
	OrderMaker *orderMaker;
	int run;

public:
	int Compare(void *firstRec, void *secRec)
	{
		SortRecordClass *firstRecord = (SortRecordClass *)firstRec;
		SortRecordClass *secondRecord = (SortRecordClass *)secRec;
		ComparisonEngine *tempCompare = new ComparisonEngine();
		return (tempCompare->Compare(firstRecord->record,secondRecord->record, secondRecord->orderMaker));
	}

	int operator()(void *firstRec, void *secRec) 
	{
		SortRecordClass *firstRecord = (SortRecordClass *)firstRec;
		SortRecordClass *secondRecord = (SortRecordClass *)secRec;
		ComparisonEngine *tempCompare = new ComparisonEngine();
		int temp = tempCompare->Compare(firstRecord->record,secondRecord->record, secondRecord->orderMaker);
		if (temp<1)
			temp=0;
		return temp;

	}

};


#endif
