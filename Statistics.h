#ifndef STATISTICS_H
#define STATISTICS_H

#include <map>
#include <string>
#include "ParseTree.h"
#include <vector>
#include <fstream.h>
#include <iostream>
using namespace std;

typedef map<string, int> att_map;
typedef struct {
	att_map attList;
	double numTuples;
} rel_struct;

typedef map <string, rel_struct> rel_map;


class Statistics {
public:	
	rel_map relationMap;
	

	Statistics(Statistics &copyMe);
	Statistics();
	void init();
//	~Statistics();

	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);

	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *toWhere);

	void Apply(struct AndList *parseTree, char **relNames, int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
	double Estimate_sum(struct AndList *parseTree, char **relNames, int numToJoin);

private:
	double calculateTupleNum(double tuplesLeft, double distinctValuesLeft, double tuplesRight, double distinctValuesRight, int opCode);
	double calculateTupleNum(double tuples, double distinctValues, int opCode);
	vector<double> GetNumTuples(char** relNames, string attrName, int numToJoin);
	string GetTableName(string attrName, char **relNames, int numToJoin);
};

#endif /* STATISTICS_H*/ 
