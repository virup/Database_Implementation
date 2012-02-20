// Include Statements
#include <iostream> 
#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <fstream>
#include "Pipe.h"
#include "RelOp.h"
#include "ParseTree.h"
#include "DBFile.h"
#include "Schema.h"
#include "Record.h"
#include "Statistics.h"
#include "QueryPlan.h"
#include "Function.h"

// Forward declarations

void doSelect();
void doCreate();
void doInsert();
void doDrop();
void doSet();
void doUpdate();

void get_cnf(char *input, Schema *left, CNF &cnf_pred, Record &literal);
void get_cnf (char *input, Schema *left, Function &fn_pred);
void inOrder(QueryTreeNode *t, int level);
int clear_pipe (Pipe &in_pipe, Schema *schema, bool print, char *fileName);
const char * GetRealName(const char * name);
void postOrderExecute(QueryTreeNode *t);

void ReadStatsFile();
void ReadPropertyFile();
void WritePropertyFile();

// Parsing Functions 
using namespace std; 
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" {
	int yyparse(void); // defined in y.tab.c
	int yyfuncparse(void);
	void init_lexical_parser_func(char *);
	void close_lexical_parser_func(void);
	//void init_lexical_parser (char *);
	//void close_lexical_parser(void);
	
}

extern struct AndList *final;
extern struct FuncOperator *finalfunc;
extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;
extern int currentPipeID;

extern char *tableName;
extern struct SchemaList *schemaList;
extern struct NameList *sortingAtts;
extern int tableType;
extern int operationType;
extern bool shutdown;

extern char *insertFileName;
extern int stdoutFlag;
extern char *outFileName; // Filename to output results

// Global Variables
char *fileRoot = "/cise/tmp/viru/";
typedef map<int, Pipe*> pipe_map;
pipe_map pipeMap;
vector<RelationalOp *> opsList;
int maxPipeID;
Pipe *pOut;
Pipe *leftPipe;
Pipe *rightPipe;
char* propertyFile = "dbi.properties";
struct PropFileStruct {
	int outputPreference; // 0 = STDOUT, 1 = file, 2 = NONE
	
} propFile;
Statistics stats;

//Main Program
int main () {
	ReadStatsFile();
	ReadPropertyFile();
	printf("Christan & Virupaksha\n");
	printf("Database Management System\n");
	printf("press ctrl-d to execute command (enter 'quit' to exit)\n\n");
	shutdown = false;
	while(!shutdown){
		printf(">>");
		yyparse();
		switch(operationType){
			case 1:
				doSelect();
				break;
			case 2:
				doCreate();
				break;
			case 3:
				doInsert();
				break;
			case 4:
				doDrop();
				break;
			case 5:
				doSet();
				break;
			case 6:
				doUpdate();
				break;
			default:
				shutdown = true;
				break;
		}
		operationType = -1; // We dont want repeat operations
	}
	printf("\n>> Good Bye!\n");
}

void doSelect(){
	maxPipeID = 0;
	// tables creation
	TableList *traverse= tables;
	vector<string> v;
	cout<<"propFile.outputPreference = "<<propFile.outputPreference<<endl;
	cout<<"Table names = "<<endl;
	while(traverse)
	{
		string str(traverse->tableName);
		v.push_back(str);
		cout<<"  "<<str<<endl;
		traverse = traverse->next;
	}
	
	//create attributes
	NameList *temp = attsToSelect;
	vector<string> v1;
	cout<<"Attribute Names = "<<endl;
	while (temp) {
		string s(temp->name);
		v1.push_back(s);
		cout<<"::  "<<s<<endl;
		temp = temp->next;
	}
	
	//Statistics s;
	//s.init();
	

	TableList * pointer = tables;

	while(pointer)
	{
		if(pointer->aliasAs != NULL)
			stats.CopyRel(pointer->aliasAs, pointer->tableName);
		pointer = pointer->next;
	}
	
	
	//stats.Read("Statistics.txt");
	QueryTreeNode *t;
	QueryTreeNode * head = NULL;
	QueryTreeNode *runningHead = NULL;
	QueryPlan ee;
		
	
	
cout<<"Grouping done ..."<<endl;
	//selection list
	NameList * tempSelect = attsToSelect;
	int selCount = 0;	// number of attributes in the selection List
	vector<string> vNameList;
	while(tempSelect)
	{
		selCount ++;
		vNameList.push_back(tempSelect->name);
		tempSelect = tempSelect->next;
	}
	
	

	
	t = ee.enumerate(v, v1, boolean, stats, 0);

	cout<<"BACK IN MAIN"<<endl;
	cout<<"Head Cost"<<t->cost<<endl;
	cout<<"vNameList.size() = "<<vNameList.size()<<endl;

	if(vNameList.size() != 0)
	{
		cout<<"t type = "<<t->opType<<endl;
		QueryTreeNode *selHead = new QueryTreeNode(PROJECT);
		selHead->KeepMe = new int[selCount];
		cout<<"t type = "<<t->opType<<endl;
		currentPipeID = t->outputPipeID;
		Schema *sc = t->schema;
		
		Attribute *Atts = t->schema->myAtts;
		cout<<"t type = "<<t->opType<<endl;
		int pos;
		Attribute *att = new Attribute[selCount];
		int count = 0, attCount = 0;
		for (int jj = 0; jj < selCount; jj++) 
		{	
			char *qw = new char[20];
			
			pos = vNameList[jj].find('.', 0);
			if(pos != -1)
			{
				strcpy(qw, vNameList[jj].substr(pos+1, vNameList[jj].length()-1).c_str());				
			}		
			else
				strcpy(qw, vNameList[jj].c_str());
		
			cout<<"---"<<qw<<endl;
			if (sc->Find(qw) != -1) 
			{
					selHead->KeepMe[count] = sc->Find(qw);
				
				att[attCount].name = sc->myAtts[sc->Find(qw)].name;
				
					att[attCount].myType = sc->myAtts[sc->Find(qw)].myType;
				
				count++;attCount ++;
			}
		}
		
		Schema *newSchema = new Schema("FinalSchema", attCount, att);
		selHead->schema = newSchema;
		selHead->right = NULL;
		selHead->leftPipeID = currentPipeID;
		selHead->outputPipeID = ++currentPipeID;
		selHead->numAttsInput = t->schema->GetNumAtts();
		if(t->opType == PROJECT)
		{
			selHead->numAttsInput = t->numAttsOutput;
			

		}
		selHead->numAttsOutput = selCount;
		selHead->left = t; // Attach the Query tree t after selection Node
		head = selHead;
			
		if(distinctAtts == 1)
		{
			cout<<"Select DISTINCTS"<<endl;
			QueryTreeNode *dup = new QueryTreeNode(DUPLICATE_REMOVAL);
			dup->schema = head->schema;
			dup->leftPipeID = head->outputPipeID;
			dup->outputPipeID = dup->leftPipeID + 1;
			currentPipeID = dup->outputPipeID;
			QueryTreeNode *temp = head;
			head = dup;
			dup->left = temp;
		}
	}
	else
	{
		head = t;
		currentPipeID--;

	}	
//GROUP BY

//DO WITH GROUPS 
	NameList *tempGroup = groupingAtts;
	QueryTreeNode *groupTree = NULL;
	if(tempGroup != NULL) 
	{
		cout<<"TempGroup NOT NULL "<<endl;
		vector<QueryTreeNode *> groupingNodes;
	
		while(tempGroup)
		{
			cout<<" -> "<<tempGroup->name<<endl;
			QueryTreeNode *q = new QueryTreeNode(GROUPBY);
			q->left = q->right = NULL;
			string strTemp;
			strTemp.clear();
			strTemp.insert(0,tempGroup->name);
			int pos = strTemp.find('.', 0);
			char *chTemp = new char[20];
			char *str_sum = new char[1024];
			if(pos != -1)
			{
				strcpy(chTemp,GetRealName(strTemp.substr(0,pos).c_str()));	
				strcpy(str_sum,strTemp.substr(pos+1,strTemp.length()-1).c_str());
			}
			Schema sc("catalog", chTemp);
			q->orderMaker = new OrderMaker(head->schema);
			cout<<"Order maker Created"<<endl;
			q->table1 = chTemp;
			
			Attribute DA = {str_sum, sc.FindType(str_sum)};
			q->schema = new Schema("sum_sch", 1, &DA);

			Function *func = new Function();
			get_cnf (str_sum, head->schema , *func);
			func->Print ();
			q->function = func;
			if(finalFunction == NULL)cout<<"Final function is NULL"<<endl;
			//q->function->GrowFromParseTree(finalFunction,sc);
			
			q->leftPipeID = currentPipeID;
			q->outputPipeID = ++currentPipeID;
			cout<<"Function Created"<<endl;
			groupingNodes.push_back(q);
			tempGroup = tempGroup->next;
		}
		
		
		for(int i = 0; i < groupingNodes.size()-1; i ++)
				groupingNodes[i]->left = groupingNodes[i+1];
		
		groupTree = groupingNodes[0];
		
	}		
		/////////////////////////////////////////////




/////////??SUM //////////////////////


	QueryTreeNode *sum = NULL;
	if(finalFunction)
	{
		if(distinctFunc == 1)
			cout<<"DISTINCT ";
		cout<<" SUM "<<endl;
		sum = new QueryTreeNode(SUM);
		sum->function = new Function();
		sum->function->GrowFromParseTree(finalFunction, *(head->schema));
		
		sum->leftPipeID = currentPipeID;
		sum->outputPipeID = sum->leftPipeID + 1;
		currentPipeID = sum->outputPipeID;
	
		Attribute DA = {"sum", Double};
		sum->schema = new Schema("sumSch", 1, &DA);
		
		
		sum->left = head;		
		
		head = sum;
	}	
	
	

	if(groupTree != NULL && sum == NULL)
	{
		cout<<"Grouping is there"<<endl;
		QueryTreeNode *pointer = groupTree;
		while(pointer->left != NULL)
			pointer = pointer->left;
		pointer->left = head;  // Attach the selection node after the last Grouping node
		head = groupTree;

	}

	if(groupTree != NULL && sum != NULL)
	{
		cout<<"Grouptree or sum != NULL"<<endl;
		QueryTreeNode *pointer = groupTree;
		while(pointer->left != NULL)
			pointer = pointer->left;
			
		pointer->left = sum;  // Attach the selection node after the last Grouping node
		head = groupTree;
	}
	
	inOrder(head, 0);
	


	cout<<"Going into postOrder "<<endl;
	postOrderExecute(head);
	cout<<"maxPipeID = "<<maxPipeID<<endl;
	cout<<"CurrentPipeID = "<<currentPipeID<<endl;
	cout<<"Outside postOrder"<<endl;
	cout<<"Outside it "<<endl;
	
	/*for(int i = 0 ; i < opsList.size(); i++)
		opsList[i]->WaitUntilDone ();
	*/
	//opsList[opsList.size()-1]->WaitUntilDone ();
	Pipe *p = pipeMap.find(currentPipeID)->second;
	//if(p == NULL) cout<<"NULL";
	

	//clear_pipe(*p,head->schema,true, NULL);

	if(propFile.outputPreference == 0)
		clear_pipe(*p,head->schema,true, NULL);
	else if(propFile.outputPreference == 2)
		clear_pipe(*p,head->schema,false, NULL);
	else
		clear_pipe(*p,head->schema,false, outFileName);


}




void doCreate(){
    printf("tablename: %s\nAtts:", tableName);
    //while(schemaList){
    //  printf("[%s,%d]",schemaList->name,schemaList->code);
    //  schemaList = schemaList->next;
    //}
    //printf("\n");

    //printf("tabletype: %s\n",(tableType==0)?"HEAP":"SORTED");
    //if(tableType==1){
    //  printf("sorting Atts: ");
    //  while(sortingAtts){
    //      printf("%s ",sortingAtts->name);
    //      sortingAtts = sortingAtts->next;
    //  }
    //  printf("\n");
    //}

    ofstream cf;
    cf.open("catalog",ios::app);
    cf << "\n";
    cf << "BEGIN\n";
    cf << tableName << '\n';
    cf << "Tables/" << tableName << ".bin\n";
    struct SchemaList *tempsl = schemaList;
    while(tempsl){
        cf << schemaList->name << " ";
        if(schemaList->code == Int){
            cf << "Int\n";
        }
        else if(schemaList->code == Double){
            cf << "Double\n";
        }
        else{
            cf << "String\n";
        }
	tempsl = tempsl->next;
    }
    cf << "END";
    cf.close();

    switch(tableType){
        case 0: // HEAP
            {
            DBFile dbfile;
            dbfile.Create (tableName, heap, NULL);
            dbfile.Close();
            printf("Heap Table %s created!\n",tableName);
            }
            break;
        case 1: // SORTED
            {

            // Get the Attributes
            int num_atts = 0;
            Attribute* alist = new Attribute[50]; // max 50 atts
            SchemaList*  runner = schemaList;
            while(runner){
                Attribute a = {runner->name,(Type)runner->code};
                alist[num_atts++] = a;
                runner = runner->next;
            }

            // Create schema object
            Schema s(tableName, num_atts, alist);
            OrderMaker o(&s);
            int runlen = 500;
            struct {OrderMaker *o; int l;} startup = {&o, runlen};

            // Create DBFile
            DBFile dbfile;
            dbfile.Create(tableName, sorted, (void*)&startup);
            dbfile.Close();
            printf("Sorted Table %s created!\n",tableName);
            }
            break;
        default:
            break;
    }
}
void doInsert(){
	// Check if the file exists
	if( access(insertFileName, R_OK)){
		printf("Insert file does not exists!\n");
		return;
	}

	Schema mySchema("catalog", tableName);
	DBFile dbfile;
	dbfile.Open(tableName);
	dbfile.MoveFirst();
	dbfile.Load(mySchema,insertFileName);
	dbfile.Close();
	cout<<"Table Inserted !"<<endl;
}

//---------------------------------------
void doDrop(){
    if(tableName == NULL)
        return;
    // remove the file
    char metaTableName[90];
    strcpy(metaTableName,tableName);
    strcat(metaTableName,".meta");
    if(remove(tableName) && remove(metaTableName)){
        printf("Error dropping %s",tableName);
    }
    else{
        printf("Table %s dropped\n",tableName);
    }
}
void doSet(){
	if(stdoutFlag ==1){
		propFile.outputPreference = 0;
	}
	else if(outFileName!=NULL){
		propFile.outputPreference = 1;
	}
	else{
		propFile.outputPreference = 2;
	}

	WritePropertyFile();
}
void doUpdate(){
	printf("Updating %s \n",tableName);
	Schema S("catalog",tableName);
	if(!true)
	for	(int currentAtt = 0; currentAtt < S.numAtts; ++currentAtt){

		int counter = 0;
		DBFile dbfile;
		dbfile.Open(tableName);
	

		Pipe _ip(100);
		Pipe _op(100);

		SelectFile SF;
		SF.Use_n_Pages(100);
		DuplicateRemoval D;
	
		//SF.Run(dbfile,_ip, 
	
		// Get Stuff from the output pipe

		SF.WaitUntilDone();
		D.WaitUntilDone();



		dbfile.Close();
	}
	printf("Statistics updated");
}

void ReadStatsFile(){
// read statsfile if it exists 
// if it doesnt exists call s.init()

	if(access("Statistics.txt", R_OK)){
		stats.init();
	}
	else{
		//stats.Read("Statistics.txt");
		stats.init();
	}

}
void ReadPropertyFile(){
	// Does the property file exist?
	propFile.outputPreference = 0;
	/*if(access(propertyFile, R_OK)){
		// It does not exist
		FILE *property_file = fopen(propertyFile, "w");
		fprintf(property_file, "%d", 0);
		fclose(property_file);
		propFile.outputPreference = 0;
	}
	else{
		// It exists
		// Get the Preference
		FILE *property_file = fopen(propertyFile, "r");
		int preference;
		fscanf(property_file,"%d",preference);
		fclose(property_file);

		propFile.outputPreference = preference;
	}
	*/

}

void WritePropertyFile(){
	FILE *property_file = fopen(propertyFile, "w");
	fprintf(property_file, "%d", propFile.outputPreference);
	fclose(property_file);
	
}

void get_cnf (char *input, Schema *left, Function &fn_pred) {
	init_lexical_parser_func (input);
	if (yyfuncparse() != 0) {
		cout << " Error: can't parse your arithmetic expr. " << input << endl;
		exit (1);
	}
	fn_pred.GrowFromParseTree (finalfunc, *left); // constructs CNF predicate
	close_lexical_parser_func ();
}

void get_cnf(char *input, Schema *left, CNF &cnf_pred, Record &literal) {
	//init_lexical_parser(input);
	//if(yyparse() != 0){
	//	cout << "error: get_cnf(char *input, Schema *left, CNF &cnf_pred, ...";
	//	exit(1);
	//}
	//cnf_pred.GrowFromParseTree(final,left,literal); //constructs CNF predicate
	//close_lexical_parser();
}

void inOrder(QueryTreeNode *t, int level)
{
	if(t==NULL)
	{
		return;
	}
	inOrder(t->left, level + 1 );
	cout<<" *****************"<<endl;
	cout<<" Level = "<<level;
	t->print();
	inOrder(t->right, level + 1 );
}

int clear_pipe (Pipe &in_pipe, Schema *schema, bool print, char *fileName) 
{
cout<<"Print = "<<print<<endl;
	if(print == false && fileName != NULL)
	{
		ofstream metaFile;
		metaFile.open(fileName);
	
	cout<<"In Clear-pipe"<<endl;
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		rec.PrintToFile (schema, metaFile);
		cnt++;
	}
	return cnt;
	}
	
	cout<<"In Clear-pipe"<<endl;
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		//cout<<"Got a rec "<<endl;
		if (print) {
			rec.Print (schema);
		}
		cnt++;
	}
	return cnt;
}

const char * GetRealName(const char * name)
{cout<<"Inside here"<<endl;
	TableList *pointer = tables;
	while(pointer)
	{
		if(strcmp(pointer->tableName,name) == 0 || strcmp(pointer->aliasAs, name) == 0)
		{
		cout<<"Found "<<endl;
			return pointer->aliasAs;
		}
		
		pointer = pointer->next;

	}
}

void postOrderExecute(QueryTreeNode *t)
{
	
	if(t == NULL)
		return ;
	cout<<"t->opType = "<<t->opType<<endl;
	//if(t->left != NULL)
	{
		//cout<<"Going Left"<<endl;
		postOrderExecute(t->left);
	}
	//else if(t->right != NULL)
	{
		//cout<<"Going Right"<<endl;
		postOrderExecute(t->right);
	}
	//else
	{
		if(t->outputPipeID > maxPipeID)
		{
			maxPipeID = t->outputPipeID;
			//cout<<" max Pipe ID = "<<maxPipeID<<endl;
		}
		if(t->opType == SELECT)
		{
			cout<<"SELECT"<<endl;
			pOut = new Pipe(100);
			cout<<"Outout PipeID = "<<t->outputPipeID<<endl;
			pipeMap.insert(make_pair<int,Pipe*>(t->outputPipeID, pOut));
			DBFile *dbFile = new DBFile();
			char *temp = new char[20];
			
			strcpy(temp, GetRealName(t->table1.c_str()));
			Schema sc("catalog", temp);
			if(sc.fileName == NULL) cout<<"NULL"<<endl;
			char * newFileName = new char[1023];
			strcpy(newFileName, fileRoot);
			strcat(newFileName, sc.fileName);
			dbFile->Open(newFileName);
			SelectFile *sf = new SelectFile();
			opsList.push_back(sf);
			sf->Use_n_Pages(100);
			sf->Run(*dbFile,*pOut,*(t->cnf),t->record);
			cout<<"*********************"<<endl;
			cout<<"Select File Operation"<<endl;
			//cout<<"Table = "<<table1<<endl;
			
			cout<<"Input Pipe ID "<<t->leftPipeID<<endl;
			cout<<"Out Pipe ID"<<t->outputPipeID<<endl;
			cout<<"Select File CNF: "<<endl;
			if(t->cnf != NULL)
			{	
				t->cnf->Print();
			}
			cout<<"*********************";

		}
		else if(t->opType == PROJECT)
		{
			cout<<"PROJECT"<<endl;
			pOut = new Pipe(100);
			pipeMap.insert(make_pair<int,Pipe*>(t->outputPipeID, pOut));
			cout<<"outputPipe ID = "<<t->outputPipeID<<endl;
			cout<<"left PipeID = "<<t->leftPipeID<<endl;
			leftPipe = pipeMap.find(t->leftPipeID)->second;
			cout<<"INput num attr = "<<t->numAttsInput<<endl;
			cout<<"output num attr = "<<t->numAttsOutput<<endl;
			Project *project = new Project();
			opsList.push_back(project);
			project->Use_n_Pages(100);
			project->Run(*leftPipe,*pOut,t->KeepMe,t->numAttsInput,t->numAttsOutput);
		
			cout<<"*********************";
		cout<<"Project Operation "<<t->table1<<endl;
		cout<<"Input Pipe ID "<<t->leftPipeID<<endl;
		cout<<"Output Pipe ID "<<t->outputPipeID<<endl;
		cout<<"Keep Me"<<endl;
		for(int jj = 0; jj < t->numAttsOutput; jj++)
		{
			cout<<t->KeepMe[jj]<<" ";
		}
		cout<<endl;
		/*cout<<"NumAttsInput"<<numAttsInput<<endl;
		cout<<"NumAttsOutput"<<numAttsOutput<<endl; 
		*/
		cout<<"*********************"<<endl;;
			//cout<<"project ran"<<endl;
		} 
		else if(t->opType == JOIN)
		{	
			cout<<"JOIN"<<endl;
			pOut = new Pipe(100);
			pipeMap.insert(make_pair<int,Pipe*>(t->outputPipeID, pOut));
			leftPipe = pipeMap.find(t->leftPipeID)->second;
			rightPipe = pipeMap.find(t->rightPipeID)->second;
			cout<<"leftPipeID = "<<t->leftPipeID<<endl;
			cout<<"rightPipeID ="<<t->rightPipeID<<endl;
			Join *join = new Join();
			opsList.push_back(join);
			join->Use_n_Pages(100);
			join->Run(*leftPipe, *rightPipe, *pOut,*(t->cnf),t->record);

			cout<<"************"<<endl;
		cout<<"Join Operation"<<endl;
		cout<<"Input pipe ID "<<t->leftPipeID<<endl;
		cout<<"Input pipe ID "<<t->rightPipeID<<endl;
		cout<<"Output pipe ID "<<t->outputPipeID<<endl;
		
			
		cout<<"Join CNF:"<<endl;
		t->cnf->Print();
		cout<<"*********************"<<endl;


		}
		else if(t->opType == GROUPBY)
		{
			cout<<"GroupBY"<<endl;
			pOut = new Pipe(1);
			cout<<"Input Pipe = "<<t->leftPipeID<<endl;
			cout<<"outputPipe = "<<t->outputPipeID<<endl;
			pipeMap.insert(make_pair<int, Pipe*>(t->outputPipeID, pOut));
			leftPipe=  pipeMap.find(t->leftPipeID)->second;
			GroupBy *gb = new GroupBy();	
			opsList.push_back(gb);
			gb->Use_n_Pages(1);
			gb->Run(*leftPipe, *pOut,*(t->orderMaker), *(t->function));
			cout<<"*********************"<<endl;
			cout<<"Group By"<<endl;
			cout<<"Input Pipe ID "<<t->leftPipeID<<endl;
			cout<<"Output Pipe ID "<<t->outputPipeID<<endl;
			cout<<"Order maker = ";
			t->orderMaker->Print();
			cout<<"Function = ";
			t->function->Print();
			cout<<"*********************"<<endl;
			
		}
		else if(t->opType == SUM)
		{
			cout<<"SUM"<<endl;
			pOut = new Pipe(1);
			cout<<"Input Pipe = "<<t->leftPipeID<<endl;
			cout<<"outputPipe = "<<t->outputPipeID<<endl;
			pipeMap.insert(make_pair<int, Pipe*>(t->outputPipeID, pOut));
			leftPipe =  pipeMap.find(t->leftPipeID)->second;
			Sum *s = new Sum();
			opsList.push_back(s);
			s->Use_n_Pages(1);
			s->Run(*leftPipe,*pOut, *(t->function));
			cout<<"*********************";
		cout<<"Sum Operation"<<endl;
		cout<<"Input Pipe ID "<<t->leftPipeID<<endl;
		cout<<"Output Pipe ID "<<t->outputPipeID<<endl;
		cout<<"Function = ";t->function->Print();
			cout<<"*********************"<<endl;

		}
		else if(t->opType == DUPLICATE_REMOVAL)
		{
			cout<<"DUPLICATE REMOVAL";
			pOut = new Pipe(100);
			cout<<"Input Pipe = "<<t->leftPipeID<<endl;
			cout<<"outputPipe = "<<t->outputPipeID<<endl;
			pipeMap.insert(make_pair<int, Pipe*>(t->outputPipeID, pOut));
			leftPipe =  pipeMap.find(t->leftPipeID)->second;
			DuplicateRemoval *dr = new DuplicateRemoval();	
			opsList.push_back(dr);
			dr->Use_n_Pages(100);
			dr->Run(*leftPipe, *pOut,*(t->schema));

		}
	}
}
