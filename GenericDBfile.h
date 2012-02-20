#ifndef GenericDBFile_H_
#define GenericDBFile_H_
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include <string>
typedef enum {heap, sorted, tree} fType;
class GenericDBfile
{
public:
	virtual void MoveFirst() =0;

	/**
	 * Is used to actually create the file, called Create. The first parameter to this function is 
	 * a text string that tells you where the binary data is physically. The second parameter to the 
	 * Create function tells you the type of the file. The return value from Create is a 1 on success
	 *  and a zero on failure.
	 */
     
	virtual int Create(char *name, fType myType, void *startup)=0;
    /**
     * This function assumes that the Heap already exists and has previously been created 
     * and then closed. The one parameter to this function is simply the physical location of
     * the file. The return value is a 1 on success and a zero on failure.
     */
	virtual int Open(char *name)=0;
    /**
     * Close simply closes the file. The return value is a 1 on success and a zero on failure.
     */ 
	virtual int Close()=0;
	/**
	 * In order to add records to the file, the function Add is used. In the case of the 
	 * unordered heap file, this function 
	 * simply adds the new record to the end of the file
	 */
	virtual void Add(Record &addMe)=0;

	/**
	 * Gets the next record from the file and returns it to the user, where “next” is defined
	 * to be relative to the current location of the pointer. After the function call returns,
	 * the pointer into the file is incremented, so a subsequent call to GetNext won’t return
	 * the same record twice. The return value is an integer whose value is zero if and only 
	 * if there is not a valid record returned from the function call (which will be the case,
	 * for example, if the last record in the file has already been returned).
	 */
	virtual int GetNext(Record &fetchMe)=0;

	/**
	 * The next version of GetNext also accepts a selection predicate (this is a conjunctive
	 * normal form expression). It returns the next record in the file that is accepted by the
	 * selection predicate. The literal record is used to check the selection predicate, and
	 * is created when the parse tree for the CNF is processed.
	 */ 
	virtual int GetNext(Record &fetchMe, CNF &applyMe, Record &literal)=0;
	
	/**
	 * Load function bulk loads the Heap instance from a text file, appending
	 * new data to it. The character string passed to Load is the name of the data file
 	 * to bulk load.
 	 */
	virtual void Load(Schema &mySchema, char *loadMe)=0;
};


#endif /*GenericDBFile_H_*/
