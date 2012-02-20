
#include"DBFile.h"

//typedef enum {heap, sorted, tree} fType;



	DBFile::DBFile(){

		myInternalVar = NULL;
	}

	void DBFile::MoveFirst()
	{
		myInternalVar->MoveFirst();
	}

	int DBFile::Create(char *name, fType myType, void *startup)
	{


		if(myType == heap)
		{
			myInternalVar = new Heap();
			//cout<<"HEAP"<<endl;
		}
		else if(myType == sorted)
		{
			myInternalVar = new Sorted();
			//cout<<"sorted"<<endl;
		}
		else
			cout<<"Err in file open"<<endl;
		myInternalVar->Create(name, myType, startup);
	}

	int DBFile::Open(char *name)
	{
	//	cout<<" DBFile::Open	"<<endl;
		char metafileName[250];
		strcpy(metafileName, name);
		strcat(metafileName, ".meta");
		ifstream meta(metafileName);
		string line;
	
		getline(meta, line);
	//cout<< " right till here" <<endl;
	//cout<< "mete = " << line;
		if (line.compare("Heap")==0) {
		//cout<<" heap file"<<endl;
			myInternalVar = new Heap();

		}
		else if(line.compare("Sorted")==0)
		{
			//cout<<" sorted file"<<endl;
			myInternalVar = new Sorted();
		}
		else{
			//cout<<"line is !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1"<<line<<endl;
		}

		myInternalVar->Open(name);
	}

	int DBFile::Close()
	{
		myInternalVar->Close();
	}

	void DBFile::Add(Record &addMe)
	{
		myInternalVar->Add(addMe);
	}

	int DBFile::GetNext(Record &fetchMe)
	{
		cout<<"asdasda sdadasdad"<<endl;
		return myInternalVar->GetNext(fetchMe);
	}


	int DBFile::GetNext(Record &fetchMe, CNF &applyMe, Record &literal)
	{
		
		myInternalVar->GetNext(fetchMe, applyMe, literal);
	}


	void DBFile::Load(Schema &mySchema, char *loadMe)
	{
		myInternalVar->Load(mySchema, loadMe);
	}
	
	





    /*
     * Destructor
     */
	DBFile:: ~DBFile()
	{

	}






