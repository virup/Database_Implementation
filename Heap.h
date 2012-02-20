#ifndef HEAP_H_
#define HEAP_H_
#include "File.h"
#include "Pipe.h"
#include "Record.h"
#include "Schema.h"
#include <string>
#include "iostream.h"
#include<fstream>
#include "Comparison.h"
#include"GenericDBfile.h"



class Heap: public GenericDBfile {
	friend class Sorted;
public:
	/**
	 * Constructor
	 */
	Heap();
	/**
	 * Each Heap instance has a pointer to the current record in the file. By default, this
	 * pointer is at the first record in the file, but it can move in response to record retrievals.
     * The following function forces the pointer to correspond to the first record in the file
     */
	void MoveFirst();

	/**
	 * Is used to actually create the file, called Create. The first parameter to this function is 
	 * a text string that tells you where the binary data is physically. The second parameter to the 
	 * Create function tells you the type of the file. The return value from Create is a 1 on success
	 *  and a zero on failure.
	 */
     
	int Create(char *name, fType myType, void *startup);
    /**
     * This function assumes that the Heap already exists and has previously been created 
     * and then closed. The one parameter to this function is simply the physical location of
     * the file. The return value is a 1 on success and a zero on failure.
     */
	int Open(char *name);
	int Open1(char *name);
    /**
     * Close simply closes the file. The return value is a 1 on success and a zero on failure.
     */ 
	int Close();
	/**
	 * In order to add records to the file, the function Add is used. In the case of the 
	 * unordered heap file, this function 
	 * simply adds the new record to the end of the file
	 */
	void Add(Record &addMe);

	/**
	 * Gets the next record from the file and returns it to the user, where next is defined
	 * to be relative to the current location of the pointer. After the function call returns,
	 * the pointer into the file is incremented, so a subsequent call to GetNext wont return
	 * the same record twice. The return value is an integer whose value is zero if and only 
	 * if there is not a valid record returned from the function call (which will be the case,
	 * for example, if the last record in the file has already been returned).
	 */
	void GetPage(Page &t, int k);
	int GetNext(Record &fetchMe);

	/**
	 * The next version of GetNext also accepts a selection predicate (this is a conjunctive
	 * normal form expression). It returns the next record in the file that is accepted by the
	 * selection predicate. The literal record is used to check the selection predicate, and
	 * is created when the parse tree for the CNF is processed.
	 */ 
	int GetNext(Record &fetchMe, CNF &applyMe, Record &literal);
	
	/**
	 * Load function bulk loads the Heap instance from a text file, appending
	 * new data to it. The character string passed to Load is the name of the data file
 	 * to bulk load.
 	 */
	void Load(Schema &mySchema, char *loadMe);
	
	/**
	 * Read from pipe and write to file till pipe ends
	 */
	void LoadfromPipe(Pipe *pipe, int c);


	int GetLength(){return file.GetLength();}

    /*
     * Destructor
     */
	virtual ~Heap();
	
	int lefttotalRecsinFile;
	int righttotalRecsinFile;
	/*File file;
	Page currentPage;
	*/
	File file;
	Page page;
private:
	

	/*
	
	Record *pointer;
	int pageNumber, currentPageNumber, firstRecord;
	char fileName[250];
	char metafileName[250];
*/
	
	Record *curRecord;
	int pageNo, curPageNo, firstRec;
	char fileName[100];
	char metaFile[100];
	

	
};

#endif /*HEAP_H_*/
