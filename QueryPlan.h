#ifndef QUERYPLAN_H
#define QUERYPLAN_H

#include "Statistics.h"
#include "Function.h"
#include "Comparison.h"
//#include "Parser.y"
#include "ParseTree.h"
#include <iostream>
#include <vector>
#include <queue>

// these data extern structures hold the result of the parsing
extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;


class QueryTreeNode;

class QueryPlan {

private:
	
	QueryTreeNode *root;
	void BuildTree();
	double min; // keep the lowest count of the Estimate_sum function
	vector<AndList *> bestPlan;
public:
	Statistics *S;
	QueryPlan();
	//QueryPlan(/* vector<string> Rels, vector<string> Atts,*/ AndList *boolean, Statistics &stats);
	~QueryPlan();
	int numberOfNames(OrList *orlist);
	bool doExpression(OrList *orlist);
	
	AndList *createAndList(vector<AndList *>);
	char **createTableNamesString(TableList *, int * num);
	QueryTreeNode *createBestTree(vector<AndList *>);



	QueryTreeNode* enumerate(vector<string>, vector<string>, AndList *, Statistics &, int);  // Builds all possible query plans And sets the best of the root 
	vector<string> getCommonAtts(vector<string> Att, vector<string> rel);
	vector<string> getAtts(AndList *b, vector<string> rel);
	vector<string> unionAttributes(vector<string> v1, vector<string> v2);
	AndList* seperateAndList(struct AndList*);
	
	const char *GetRealName(const char *name); // get the name of the actual relation it is an alias of
	Schema *createSchema(QueryTreeNode *q); // Get the schema coming from the node q
	char *GetNameWithoutTable(char* name); // Get name of attribute without the table name attached.
	int checkAttPresent(struct AndList *parseTree, Schema *leftSchema,Schema *rightSchema);// Check if the attrs in AndList is there in the schema or not.
	AndList *makeAndListAll(Schema *s); // make a always TRUE AndList
	
};

enum Operation { SELECT, SELECT_PIPE, PROJECT, JOIN, SUM, GROUPBY,DUPLICATE_REMOVAL, NONE};

class QueryTreeNode {

public:

	QueryTreeNode();
	QueryTreeNode(Operation opType);

	Operation opType;
	bool valid;
	double cost;
	
	int leftPipeID;
	int rightPipeID;
	int outputPipeID;
	
	string table1;
	string table2;
	Record record;
	
	int *KeepMe; // attrs for project
	int numAttsInput; // for Project
	int numAttsOutput; // for Project

	Schema *schema; //schema for output
	CNF *cnf; // select, select pipe, join
	OrderMaker *orderMaker; // group by
	Function *function; // sum, group by
	
	QueryTreeNode *left; // child
	QueryTreeNode *right; // child

	void print();
	char *toString();




	
};

class RelationPlanWrapper
{
	
public:
	vector<string> v1;
	vector<string> v2;
	//RelationPlanWrapper();
	//void create();
	
};
#endif /* QUERYPLAN_H */
