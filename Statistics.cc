#include "Statistics.h"
#include "math.h"

/*
	typedef map<string, int> att_map;
	typedef struct {
		att_map attList;
		int numTuples;
	} rel_struct;
	
	typedef map <string, rel_struct> rel_map;

	att_map attributeMap;
	rel_map relationMap;

*/
Statistics::Statistics(Statistics &copyMe)
{
	rel_map ::iterator rel_map_it;
	att_map::iterator att_map_it;
	
	rel_map_it = copyMe.relationMap.begin();

	rel_map newRelationMap;
	
	while(rel_map_it != copyMe.relationMap.end())
	{
		rel_struct temp = rel_map_it->second;
		att_map_it = temp.attList.begin();
		att_map newAttributeMap;

		while(att_map_it != temp.attList.end())
		{
			newAttributeMap.insert(pair<string, int>(att_map_it->first,att_map_it->second));
			att_map_it++;
		}
		rel_struct newRelStruct;
		newRelStruct.attList = newAttributeMap;
		newRelStruct.numTuples = temp.numTuples;
		relationMap.insert(pair<string, rel_struct>(rel_map_it->first,newRelStruct));
		rel_map_it ++;
	}

}
Statistics::Statistics(){}
//Statistics::~Statistics(){}

void Statistics::AddRel(char *relName, int numTuples)
{
	rel_struct newRelStruct;
	newRelStruct.numTuples = numTuples;
	relationMap.insert(pair<string, rel_struct>(string(relName),newRelStruct));
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
	rel_map::iterator it;
	it = relationMap.find(relName);
	
	rel_struct newRelStruct = it->second;
	(it->second).attList.insert(pair<string,int>(string(attName),numDistincts));
	
}
void Statistics::CopyRel(char *oldName, char *newName)
{
	rel_map::iterator it;
	it = relationMap.find(oldName);
	rel_struct newRelStruct = it->second;

	relationMap.insert(pair<string, rel_struct>(string(newName),newRelStruct));

}

void Statistics::Read(char *fromWhere){
	relationMap.clear();

	FILE *out = fopen(fromWhere, "r");
	char line[400];
	char relation_name[25];
	double num_tuples;
	int num_atts;

	// Read each line
	while(fgets(line,398,out)){

		char *ptr;
		ptr = strtok(line,"|"); // get the relation name
		strcpy(relation_name,ptr);

		ptr = strtok(NULL,"|"); // get the number of tuples
		if(ptr == NULL || ptr == "0" || ptr == "0.0")
			num_tuples = 0.0;
		else
			num_tuples = atof(ptr);

		ptr = strtok(NULL,"|"); // get the number of atts
		if(ptr == NULL || ptr == "0")
			num_atts = 0;			
		else
			num_atts = atoi(ptr);

		// now get the rest of the the line and process
		att_map tempMap;

		char att_name[50];
		int num_distincts;

		ptr = strtok(NULL,"|");
		while(ptr != NULL && strcmp(ptr,"\n")!=0) {
			strcpy(att_name,ptr);

			ptr = strtok(NULL,"|");
			num_distincts = atoi(ptr);
		
			tempMap.insert( make_pair(att_name, num_distincts) );
			ptr = strtok(NULL,"|");
		}
		
		rel_struct rs;
		rs.attList = tempMap;
		rs.numTuples = num_tuples;

		relationMap.insert( make_pair(string(relation_name),rs) );
	}	

	fclose(out);
}

void Statistics::Write(char *toWhere){
	FILE *out = fopen(toWhere, "w");

	rel_map::iterator rel_map_it;
	for(rel_map_it = relationMap.begin(); rel_map_it != 
		relationMap.end(); ++rel_map_it){

		string relation = rel_map_it->first;
		rel_struct _rel_struct = rel_map_it->second;

		fprintf(out,"%s|",relation.c_str()); // relation name|
		fprintf(out,"%lf|",(0+_rel_struct.numTuples)); // num_tuples|
		fprintf(out,"%d|",(0+_rel_struct.attList.size())); //num_atts|

		att_map::iterator att_map_it;
		for(att_map_it = _rel_struct.attList.begin(); att_map_it != 
			_rel_struct.attList.end(); ++att_map_it){

			fprintf(out,"%s|",att_map_it->first.c_str()); // att_name|
			fprintf(out,"%d|",att_map_it->second); // num_distincts|

		}
		fprintf(out,"\n");
	}
	fclose(out);
}



vector<double> Statistics::GetNumTuples(char** relNames, string attrName, int numToJoin)
{
	vector<double> v;
	att_map ::iterator attmapPointer;
	rel_map::iterator relmapPointer;
	
	for(int i=0;i<numToJoin;i++)
	{
		string rName(relNames[i]);

		relmapPointer = relationMap.find(rName);
		if( relmapPointer!=relationMap.end())
		{

			string s(attrName);
			attmapPointer = relmapPointer->second.attList.find(s);

			if(attmapPointer!=relmapPointer->second.attList.end())
			{
				v.push_back(relmapPointer->second.numTuples);
				v.push_back(attmapPointer->second);
				return v;
			}
			
		}
	}
	cout<<" error in GetNumTables: table not found."<<endl;

}

string Statistics::GetTableName(string attrName, char **relNames, int numToJoin){
		rel_map ::iterator relmapPointer;
		att_map ::iterator attmapPointer;
	
		for(int i = 0 ; i < numToJoin; i++)
		{
			string findInRel(relNames[i]);
			
			relmapPointer = relationMap.find(findInRel);
			if(relmapPointer == relationMap.end())
			{
				cout<< relNames[i]<<" NOT FOUND in get table name!!"<<endl;
				exit(0);
			}
			attmapPointer = relmapPointer->second.attList.find(attrName);
			if(attmapPointer != relmapPointer->second.attList.end())
				return findInRel;
		}

		//cout<<attrName<< " not found ! "<<endl;				
}


double Statistics::calculateTupleNum(double tuples, double distinctValues,  int opCode)
{
	if(opCode == LESS_THAN || opCode == GREATER_THAN){
		//cout << "calculateTupleNum1a: " << tuples/3.0 << endl;
		return tuples/3.0;
	}
	else{
		//cout << "calculateTupleNum1b1: " << ((double)tuples)/distinctValues << endl;
		return ((double)tuples)/distinctValues;	
	}
}

double Statistics::calculateTupleNum(double tuplesLeft, double distinctValuesLeft, double tuplesRight, double distinctValuesRight, int opCode)
{
	if(opCode == EQUALS) {
		//cout << "calculateTupleNum2a: " << ((double)tuplesLeft * (double)tuplesRight)/max(distinctValuesLeft, distinctValuesRight) << endl;
		
		return ((double)tuplesLeft * (double)tuplesRight)/max(distinctValuesLeft, distinctValuesRight);
	}
	else{
		//cout << "calculateTupleNum2b: " << ((double)tuplesLeft * (double)tuplesRight) << endl;
		return ((double)tuplesLeft * (double)tuplesRight);
	}
}


void Statistics::Apply(struct AndList *parseTree, char **relNames, int numToJoin)
{
	//cout<<" In estimate : "<<endl;

	struct AndList *parsePointer = parseTree;
	int opCode;
	double sum = 0.0;
	char *chTemp;
	string strTemp;
	string attrLeft, attrRight;
	string tableLeft;
	string tableRight;

	//Statistics S(*this);
	
	vector<double> vectorLeft, vectorRight;
	vector<double> orSum;
	double JoinNoTuples = 0;

	//INITIALIZED here
	int flagLeft =0, flagRight = 0, joinFlag = 0;
	vector<double> andRes, andFactors;
	vector <string> joinTableNames;
	int joinTableCounter = 0;
	
	rel_map ::iterator relmapPointer;
	att_map ::iterator attmapPointer;

	while(parsePointer)
	{
		sum = 0;
		//cout<<" Inside while" <<endl;
		opCode = parsePointer->left->left->code;
		//cout<<" opCode = "<<opCode<<endl;
		if(parsePointer ->left->left->right->code == NAME)
		{
		//	cout<<" Inside if"<<endl;
			joinFlag = 1;
			//chTemp = parsePointer ->left->left->left->value;
			strTemp.clear();
			strTemp.insert(0,parsePointer ->left->left->left->value);
		//	cout<<" strTemp = " << strTemp<<endl;
			int pos = strTemp.find('.', 0);
		//	cout<<"pos = "<<pos;
			if(pos != -1)
			{
				flagLeft= 1;
				tableLeft = strTemp.substr(0, pos);
				attrLeft = strTemp.substr(pos+1, strTemp.length()-1);				
			}
			else{
				attrLeft.clear();
				attrLeft.insert(0,parsePointer ->left->left->left->value);
			}
			strTemp.clear();
			strTemp.insert(0,parsePointer ->left->left->right->value);
		//	cout<<" strTemp = " << strTemp<<endl;
			pos = strTemp.find('.', 0);
		//	cout<<"pos = "<<pos;
			if(pos != -1)
			{
				flagRight = 1;
				tableRight = strTemp.substr(0, pos);
				attrRight = strTemp.substr(pos+1, strTemp.length()-1);				
			}
			else{
				attrRight.clear();
				attrRight.insert(0,parsePointer ->left->left->right->value);
			}
		//	cout<<"flagLeft = "<<flagLeft<<endl;
			if(flagLeft)
			{
				vectorLeft.push_back((relationMap.find(tableLeft)->second).numTuples); // get the total number of tuples in that relation
				vectorLeft.push_back(relationMap.find(tableLeft)->second.attList.find(attrLeft)->second); //no of dist values of that attr
			}
			else
			{
				vectorLeft = GetNumTuples(relNames, attrLeft, numToJoin);
				tableLeft = GetTableName(attrLeft,relNames, numToJoin);
			}


		//	cout<<"flagRight = "<<flagRight<<endl;
			if(flagRight)
			{
				vectorRight.push_back((relationMap.find(tableRight)->second).numTuples);// get the total number of tuples in that relation
				vectorRight.push_back(relationMap.find(tableRight)->second.attList.find(attrRight)->second);//no of dist values of that attr
			}
			else
			{
				vectorRight = GetNumTuples(relNames, attrRight, numToJoin);
				tableRight = GetTableName(attrRight,relNames,numToJoin);
			}

			//TODO: calculateValue function			
			// CHANGED HERE 3rd arg
		//	cout<<parsePointer ->left->left->left->value<<" "<<parsePointer ->left->left->right->value<<endl;
			JoinNoTuples = calculateTupleNum(vectorLeft[0], vectorLeft[1], vectorRight[0], vectorRight[1], opCode);
		//	cout<<" JointNoTuples = "<<JoinNoTuples<<endl;
			joinTableNames.push_back(tableLeft);joinTableCounter++;//]=tableLeft;
			joinTableNames.push_back(tableRight);joinTableCounter++;//]=tableRight;
			relationMap.find(tableLeft)->second.numTuples = JoinNoTuples;
			relationMap.find(tableRight)->second.numTuples = JoinNoTuples;
			
			relmapPointer  = relationMap.find(tableRight);

			// Get the minimum distint tuple count from the common join attribute
			double themin = min( (double)relationMap.find(tableLeft)->second.attList.find(attrLeft)->second,
						(double)relationMap.find(tableRight)->second.attList.find(attrRight)->second);
						
			// Apply minimum to both, just to be sure
			relationMap.find(tableRight)->second.attList.find(attrRight)->second = themin;
			//relationMap.find(tableLeft)->second.attList.find(attrLeft)->second = themin;
			
			for ( attmapPointer=relmapPointer->second.attList.begin() ; attmapPointer != relmapPointer->second.attList.end(); attmapPointer++)
			{
				// Adding the attributes for this table
				relationMap.find(tableLeft)->second.attList.insert(pair<string, int>(attmapPointer->first,attmapPointer->second));
			}

			relationMap.find(tableRight)->second.attList.clear();
			relationMap.find(tableRight)->second.attList = relationMap.find(tableLeft)->second.attList;
			
			vectorLeft.clear();
			vectorRight.clear();
		}
		else
		{
		//	cout<<" Inside else"<<endl;
			flagLeft = 0;
			flagRight = 0;
			strTemp.clear();
			strTemp.insert(0,parsePointer ->left->left->left->value);
			int pos = strTemp.find('.', 0);
			if(pos != -1)
			{
				flagLeft= 1;
				tableLeft.clear();
				tableLeft = strTemp.substr(0, pos);
				attrLeft = strTemp.substr(pos+1, strTemp.length()-1);				
			}
			else{
				attrLeft.clear();
				attrLeft.insert(0,parsePointer ->left->left->left->value);
			}


			if(flagLeft)
			{
				vectorLeft.push_back((relationMap.find(tableLeft)->second).numTuples); // get the total number of tuples in that relation
				vectorLeft.push_back(relationMap.find(tableLeft)->second.attList.find(attrLeft)->second); //no of dist values of that attr
			}
			else
			{
				vectorLeft = GetNumTuples(relNames, attrLeft, numToJoin);
				tableLeft = GetTableName(attrLeft,relNames, numToJoin);
			}

		//	cout <<".";
		//	cout<<" vector left [0] = "<<vectorLeft[0];
		//	cout<<" vector left [1] = "<<vectorLeft[1]<<endl;
		//	cout<<parsePointer ->left->left->left->value<<" "<<parsePointer ->left->left->right->value<<endl;
			sum = calculateTupleNum(vectorLeft[0], vectorLeft[1], opCode);
			orSum.push_back(sum);
		//	cout << "sum: " << sum << endl;
			vectorLeft.clear();	
		}
		
		
		
	// Add factor stuff
		vector<string> orAttNames;
		vector<double> orFactor;
		
		if(opCode == EQUALS){
			orFactor.push_back(3.0);
		}
		else {
			orFactor.push_back(vectorLeft[1]);
		}
		orAttNames.push_back(attrLeft);
		
		
		double compositeFactor = 0.0;
		bool isComposite = false;
		if(parsePointer->left->rightOr)
		{
		//	cout<<" Inside parsePointer ->left->rightOR"<<endl;
			struct OrList *traverseOR = parsePointer->left->rightOr;
			isComposite = true;
			while(traverseOR)
			{
			//	cout<<"Inside traverseOR"<<endl;
				opCode = traverseOR->left->code;
			//	cout<<"OpCode = "<<opCode<<endl;
				flagLeft = 0;
				flagRight = 0;
				strTemp.clear();
				strTemp.insert(0,traverseOR->left->left->value);
				int pos = strTemp.find('.', 0);
				if(pos != -1)
				{
					flagLeft= 1;
					tableLeft.clear();
					tableLeft = strTemp.substr(0, pos);
					attrLeft.clear();
					attrLeft = strTemp.substr(pos+1, strTemp.length()-1);				
				}
				else{
					attrLeft.clear();
					attrLeft.insert(0,traverseOR->left->left->value);
				}

				if(flagLeft)
				{
					vectorLeft.push_back((relationMap.find(tableLeft)->second).numTuples); // get the total number of tuples in that relation
					vectorLeft.push_back(relationMap.find(tableLeft)->second.attList.find(attrLeft)->second); //no of dist values of that attr
				}
				else
				{
					vectorLeft.clear();
					tableLeft.clear();
					vectorLeft = GetNumTuples(relNames, attrLeft, numToJoin);
					tableLeft = GetTableName(attrLeft,relNames, numToJoin);
				}

		//	cout <<".";
		//	cout<<" vector left [0] = "<<vectorLeft[0];
		//	cout<<" vector left [1] = "<<vectorLeft[1]<<endl;
				//cout<<traverseOR->left->left->value<<" "<<traverseOR->left->right->value<<endl;
		//		cout<<" CalculateTupleNum ="<< calculateTupleNum(vectorLeft[0], vectorLeft[1], opCode)<<endl;
				if(opCode == EQUALS)
					orFactor.push_back(3.0);
				else {
					orFactor.push_back(vectorLeft[1]);
				}
				orAttNames.push_back(attrLeft);
		
				sum = calculateTupleNum(vectorLeft[0], vectorLeft[1], opCode);
				orSum.push_back(sum);
				if(orSum.size() == 2){
					if(orAttNames[0] != orAttNames[1]){
						double m1 , m2;
						m1= orSum[0];
						m2= orSum[1];
						double n = vectorLeft[0];
						sum = n * (1.0-(1.0-m1/n)*(1.0-m2/n));
						
					}
					else {
						// the attributes are the same
						double n = vectorLeft[0];
						sum = n *( (1/orFactor[0]) + (1/orFactor[1]));
					}
					
					orSum.clear();
				}
				vectorLeft.clear();		
				traverseOR=traverseOR->rightOr;
			}

		}
		if(sum > 0.0)
		{
			relationMap.find(tableLeft)->second.numTuples = sum;
			
			if(joinFlag == 1)
			{
				
				for(int z=0; z<joinTableCounter; z++)
					relationMap.find(joinTableNames[z].c_str())->second.numTuples = sum;
			}
			andRes.push_back(sum);
		//}

		// Calculate the factors
		int code = parsePointer->left->left->code; // EQUALS LESS_THAN GREATER_THAN
		if(code == EQUALS){
			int daType = parsePointer->left->left->right->code;
			if(daType == NAME){
				string leftName = parsePointer->left->left->left->value;
				if(leftName.find(".") != string::npos){
					// Contains a period, remove it ald all that preceeds it
					string temp (leftName.substr(leftName.find(".")+1));
					leftName.swap( temp );
				}

				string rightName = parsePointer->left->left->right->value;
				if(rightName.find(".") != string::npos){
                    // Contains a period, remove it ald all that preceeds it
					string temp(rightName.substr(rightName.find(".")+1));
                    rightName.swap(temp );
                }

				string leftTable = GetTableName(leftName,relNames, numToJoin);
				double leftV = relationMap.find(leftTable)->second.attList.find(leftName)->second;

				string rightTable = GetTableName(rightName,relNames, numToJoin);
				double rightV = relationMap.find(rightTable)->second.attList.find(rightName)->second;

				// add the max of the factors
				andFactors.push_back(max((double)leftV,(double)rightV));
			}
			else {
				string leftName = parsePointer->left->left->left->value;
				if(leftName.find(".") != string::npos){
                    // Contains a period, remove it ald all that preceeds it
                    string temp (leftName.substr(leftName.find(".")+1));
                    leftName.swap( temp );
                }
			
				string leftTable = GetTableName(leftName,relNames, numToJoin);
                double leftV = relationMap.find(leftTable)->second.attList.find(leftName)->second;

				// Get the factor
				andFactors.push_back(leftV);	
			}
		}
		else{
			// Estimate for > or <
			andFactors.push_back(3.0);
		}
		}
		parsePointer = parsePointer->rightAnd;
	}
	
	if(sum == 0){
		//return JoinNoTuples;
	}
	else
	{
		//printf("APPLY-------------------------------\n");
		//printf("andRes: ");
		for(int i = 0 ; i < andRes.size();i++)
		{
			//cout << andRes[i] << "," ;
		} //printf("\n");
		
		//printf("andComposite: ");
		//for(int i = 0 ; i < andComposite.size();i++)
		//{
		//	cout << andComposite[i] << ",";
		//} printf("\n");
		
		//printf("andFactors: ");
		for(int i = 0 ; i < andFactors.size();i++)
		{
			//cout << andFactors[i] << ",";
		}// printf("\n");
		//printf("-------------------------------\n");
		double min = andRes[0];
		for(int i = 1 ; i < andRes.size();i++)
		{
			if(andRes[i] < min)
				min = andRes[i];
		}
		//return min;
	}
	
}




double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
	cout<<"Inside estimate"<<endl;
	struct AndList *parsePointer = parseTree;
	int opCode;
	double sum = 0.0;
	char *chTemp;
	string strTemp;
	string attrLeft, attrRight;
	string tableLeft;
	string tableRight;

	Statistics S(*this);
	
	vector<double> vectorLeft, vectorRight;
	vector<double> orSum;
	double JoinNoTuples = 0.0;
	int flagLeft =0, flagRight = 0, joinFlag = 0;
	vector<double> andRes, andFactors;
	vector<bool> andComposite;
	vector <string> tableLeftList;
	vector <string> joinTableNames;
	int joinTableCounter = 0;
	
	// Add factor stuff
	vector<std::string> orAttrNames;
	vector<double> orFactor;
	rel_map ::iterator relmapPointer;
	att_map ::iterator attmapPointer;
	bool isDifferent = false;

	while(parsePointer)
	{
		sum = 0;
		opCode = parsePointer->left->left->code;
		if(parsePointer ->left->left->right->code == NAME)
		{
			joinFlag = 1;
			strTemp.clear();
			strTemp.insert(0,parsePointer ->left->left->left->value);
			int pos = strTemp.find('.', 0);
			if(pos != -1)
			{
				flagLeft= 1;
				tableLeft = strTemp.substr(0, pos);
				attrLeft = strTemp.substr(pos+1, strTemp.length()-1);				
			}
			else{
				attrLeft.clear();
				attrLeft.insert(0,parsePointer ->left->left->left->value);
			}
			strTemp.clear();
			strTemp.insert(0,parsePointer ->left->left->right->value);
			pos = strTemp.find('.', 0);
			if(pos != -1)
			{
				flagRight = 1;
				tableRight = strTemp.substr(0, pos);
				attrRight = strTemp.substr(pos+1, strTemp.length()-1);				
			}
			else{
				attrRight.clear();
				attrRight.insert(0,parsePointer ->left->left->right->value);
			}
			if(flagLeft)
			{
				vectorLeft.push_back((S.relationMap.find(tableLeft)->second).numTuples); // get the total number of tuples in that relation
				vectorLeft.push_back(S.relationMap.find(tableLeft)->second.attList.find(attrLeft)->second); //no of dist values of that attr
			}
			else
			{
				vectorLeft = S.GetNumTuples(relNames, attrLeft, numToJoin);
				tableLeft = S.GetTableName(attrLeft,relNames, numToJoin);
			}
			if(flagRight)
			{
				vectorRight.push_back((S.relationMap.find(tableRight)->second).numTuples);// get the total number of tuples in that relation
				vectorRight.push_back(S.relationMap.find(tableRight)->second.attList.find(attrRight)->second);//no of dist values of that attr
			}
			else
			{
				vectorRight = S.GetNumTuples(relNames, attrRight, numToJoin);
				tableRight = S.GetTableName(attrRight,relNames,numToJoin);
			}

			JoinNoTuples = calculateTupleNum(vectorLeft[0], vectorLeft[1], vectorRight[0], vectorRight[1], opCode);
			joinTableNames.push_back(tableLeft);joinTableCounter++;//]=tableLeft;
			joinTableNames.push_back(tableRight);joinTableCounter++;//]=tableRight;
			S.relationMap.find(tableLeft)->second.numTuples = JoinNoTuples;
			S.relationMap.find(tableRight)->second.numTuples = JoinNoTuples;
			
			relmapPointer  = S.relationMap.find(tableRight);

			// Get the minimum distint tuple count from the common join attribute
			double themin = min( S.relationMap.find(tableLeft)->second.attList.find(attrLeft)->second,
						S.relationMap.find(tableRight)->second.attList.find(attrRight)->second);
						
			// Apply minimum to both, just to be sure
			S.relationMap.find(tableRight)->second.attList.find(attrRight)->second = themin;
			
			for ( attmapPointer=relmapPointer->second.attList.begin() ; attmapPointer != relmapPointer->second.attList.end(); ++attmapPointer)
				S.relationMap.find(tableLeft)->second.attList.insert(pair<string, int>(attmapPointer->first,attmapPointer->second));
			

			S.relationMap.find(tableRight)->second.attList.clear();
			S.relationMap.find(tableRight)->second.attList = S.relationMap.find(tableLeft)->second.attList;
			
			vectorLeft.clear();
			vectorRight.clear();
		}
		else
		{
			flagLeft = 0;
			flagRight = 0;
			strTemp.clear();
			//JoinNoTuples = 0.0;
			isDifferent= true;
			strTemp.insert(0,parsePointer ->left->left->left->value);
			int pos = strTemp.find('.', 0);
			if(pos != -1)
			{
				flagLeft= 1;
				tableLeft.clear();
				tableLeft = strTemp.substr(0, pos);
				attrLeft = strTemp.substr(pos+1, strTemp.length()-1);				
			}
			else{
				attrLeft.clear();
				attrLeft.insert(0,parsePointer ->left->left->left->value);
			}


			if(flagLeft)
			{
				vectorLeft.push_back(S.relationMap.find(tableLeft)->second.numTuples); // get the total number of tuples in that relation
				vectorLeft.push_back(S.relationMap.find(tableLeft)->second.attList.find(attrLeft)->second); //no of dist values of that attr
			}
			else
			{
				vectorLeft = S.GetNumTuples(relNames, attrLeft, numToJoin);
				tableLeft = S.GetTableName(attrLeft,relNames, numToJoin);
			}

			//cout<<parsePointer ->left->left->left->value<<" "<<parsePointer ->left->left->right->value<<endl;
			//cout<<" vector left [0] = "<<vectorLeft[0];
			//cout<<" vector left [1] = "<<vectorLeft[1]<<endl;
			
			if(parsePointer->left->rightOr == NULL)
			{
				sum = S.calculateTupleNum(vectorLeft[0], vectorLeft[1], opCode);
				orSum.push_back(sum);
				//cout << "sum: " << sum << endl;
				vectorLeft.clear();	
			}
			else
			{
				if(opCode == EQUALS)
					orFactor.push_back(vectorLeft[1]);
				else
					orFactor.push_back(3.0);
				orAttrNames.push_back(attrLeft);

			}
		}
		
		
		
		
		
		
		double compositeFactor = 0.0;
		bool isComposite = false;
		if(parsePointer->left->rightOr)
		{
			struct OrList *traverseOR = parsePointer->left->rightOr;
		
			isComposite = true;
			while(traverseOR)
			{

				
		
				
				flagLeft = 0;
				flagRight = 0;
				strTemp.clear();
				strTemp.insert(0,traverseOR->left->left->value);
				int pos = strTemp.find('.', 0);
				if(pos != -1)
				{
					flagLeft= 1;
					tableLeft.clear();
					tableLeft = strTemp.substr(0, pos);
					attrLeft.clear();
					attrLeft.insert(0,strTemp.substr(pos+1, strTemp.length()-1));
				}
				else{
					attrLeft.clear();
					attrLeft.insert(0,traverseOR->left->left->value);
				}

				if(flagLeft){
					vectorLeft.push_back((S.relationMap.find(tableLeft)->second).numTuples); // get the total number of tuples in that relation
					vectorLeft.push_back(S.relationMap.find(tableLeft)->second.attList.find(attrLeft)->second); //no of dist values of that attr
				}
				else{
					vectorLeft.clear();
					tableLeft.clear();
					vectorLeft = S.GetNumTuples(relNames, attrLeft, numToJoin);
					tableLeft = S.GetTableName(attrLeft,relNames, numToJoin);
				}
				
				opCode = traverseOR->left->code;
				if(opCode == EQUALS)
					orFactor.push_back(vectorLeft[1]);
				else
					orFactor.push_back(3.0);
				orAttrNames.push_back(attrLeft);
				
				//cout<<parsePointer ->left->left->left->value<<" "<<parsePointer ->left->left->right->value<<endl;
				//cout<<" vector left [0] = "<<vectorLeft[0];
				//cout<<" vector left [1] = "<<vectorLeft[1]<<endl;
				sum = calculateTupleNum(vectorLeft[0], vectorLeft[1], opCode);
				orSum.push_back(sum);
				
				vectorLeft.clear();		
				traverseOR=traverseOR->rightOr;
				
				
			}


			if(orAttrNames.size() == 3)
			{
				//cout<<"Inside orAttrNames = 3"<<endl;
				if(orAttrNames[0] != orAttrNames[1] && orAttrNames[1] != orAttrNames[2] && orAttrNames[0] != orAttrNames[2] )
				{
		
					double m1 = orSum[0];
					double m2 = orSum[1];
					double m3 = orSum[2];
					double n = vectorLeft[0];
					sum =  n * (1.0-(1.0-m1/n)*(1.0-m2/n)*(1.0-m3/n));
				}
				else if(orAttrNames[0] == orAttrNames[1] && orAttrNames[1] == orAttrNames[2] && orAttrNames[0] == orAttrNames[2] )
				{
					double n = vectorLeft[0];
					sum =  n * (1/orFactor[0] + 1/orFactor[1] + 1/orFactor[2]);
	
				}
			}
			else if(orAttrNames.size() == 2)
			{
				//cout<<"Inside orAttrNames = 2::: orAttrNames = "<<orAttrNames[0]<<"   orAttrNames[1] = "<<orAttrNames[1]<<endl;
				if(orAttrNames[0] != orAttrNames[1])
				{
					//cout<<"Names not equal"<<endl;
					double m1 , m2;
					m1= orSum[0];
					m2= orSum[1];
					double n = vectorLeft[0];
					//cout<<"m1 = "<<m1<<"  m2 = "<<m2<<"   n = "<<n<<endl;
					sum = n * (1.0-(1.0-m1/n)*(1.0-m2/n));
						
				}
				else 
				{
					// the attributes are the same
					double n = vectorLeft[0];
					//cout<<"Factors are : orFactor[0] = " <<orFactor[0]<<"  orFactor[1] = "<<orFactor[1]<<"   n="<<n<<endl;
					sum = n *( (1/orFactor[0]) + (1/orFactor[1]));
				}
			}
		}
		if(sum > 0.0)
		{
			/*cout<<"Write out the changes in the numbers "<<endl;
			S.relationMap.find(tableLeft)->second.numTuples = sum;
			
			if(joinFlag == 1)
			{
				for(int z=0; z<joinTableCounter; z++)
					S.relationMap.find(joinTableNames[z].c_str())->second.numTuples = sum;
			}
*/
			
			
		andRes.push_back(sum);
			
		}
		else
			andRes.push_back(JoinNoTuples);

		if(andRes.size() == 2 &&  parsePointer->rightAnd != NULL)
		{

			double totalTuples = S.relationMap.find(tableLeft)->second.numTuples;
			//cout<<"Total tuples = "<<totalTuples<<endl;
			double totalFactors = 2 ;

			for(int i = 0 ; i < andRes.size();i++)
				totalFactors *= andRes[i];
			double answer = totalFactors/totalTuples;
			//cout<<"answer = "<<answer<<endl;
			
			//cout<<"Write out the changes in the numbers "<<endl;
			S.relationMap.find(tableLeft)->second.numTuples = answer;
			
			if(joinFlag == 1)
			{
				for(int z=0; z<joinTableCounter; z++)
					S.relationMap.find(joinTableNames[z].c_str())->second.numTuples = answer;
			}
		andRes.clear();

		}
		parsePointer = parsePointer->rightAnd;
	}
	
	//if(sum == 0){
	if(isDifferent == false){
		//cout << "Sum == 0 \n";
		return JoinNoTuples;
	}
	else{
	
		
	
		//printf("-------------------------------\n");
		//printf("andRes: ");
		for(int i = 0 ; i < andRes.size();i++)
		{
			//cout << andRes[i] << "," ;
		}// printf("\n");
		
		//printf("andComposite: ");
		for(int i = 0 ; i < andComposite.size();i++)
		{
			//cout << andComposite[i] << ",";
		} //printf("\n");
		
		//printf("andFactors: ");
		for(int i = 0 ; i < andFactors.size();i++)
		{
			//cout << andFactors[i] << ",";
		}// printf("\n");
		//printf("-------------------------------\n");
	/*	
		// What now??
		double min = andRes[0];
		double total = 0.0;
		for(int i = 1 ; i < andRes.size();i++)
		{
			//andRes[i]
			if(andRes[i] < min)
				min = andRes[i];
		}
		return min;
		*/
		if(andRes.size() == 1) return andRes[0];
		double totalTuples = S.relationMap.find(tableLeft)->second.numTuples;
		//cout<<"Total tuples = "<<totalTuples<<endl;
		double totalFactors = 1 ;
		for(int i = 0 ; i < andRes.size();i++)
			totalFactors *= andRes[i];
		double answer = totalFactors/totalTuples;
		//cout<<"answer = "<<answer<<endl;	
		return answer;
	}
	
}








double Statistics::Estimate_sum(struct AndList *parseTree, char **relNames, int numToJoin)
{

	cout<<"Inside estimate_Sum"<<endl;
	
	struct AndList *parsePointer = parseTree;
	int opCode;
	double sum = 0.0;
	char *chTemp;
	string strTemp;
	string attrLeft, attrRight;
	string tableLeft;
	string tableRight;
	int total_sum = 0; 
	Statistics S(*this);
	
	vector<double> vectorLeft, vectorRight;
	vector<double> orSum;
	double JoinNoTuples = 0.0;
	int flagLeft =0, flagRight = 0, joinFlag = 0;
	vector<double> andRes, andFactors;
	vector<bool> andComposite;
	vector <string> tableLeftList;
	vector <string> joinTableNames;
	int joinTableCounter = 0;
	
	// Add factor stuff
	vector<std::string> orAttrNames;
	vector<double> orFactor;
	rel_map ::iterator relmapPointer;
	att_map ::iterator attmapPointer;
	bool isDifferent = false;

	while(parsePointer)
	{
		sum = 0;
		opCode = parsePointer->left->left->code;
		if(parsePointer ->left->left->right->code == NAME)
		{
			joinFlag = 1;
			strTemp.clear();
			strTemp.insert(0,parsePointer ->left->left->left->value);
			int pos = strTemp.find('.', 0);
			if(pos != -1)
			{
				flagLeft= 1;
				tableLeft = strTemp.substr(0, pos);
				attrLeft = strTemp.substr(pos+1, strTemp.length()-1);				
			}
			else{
				attrLeft.clear();
				attrLeft.insert(0,parsePointer ->left->left->left->value);
			}
			strTemp.clear();
			strTemp.insert(0,parsePointer ->left->left->right->value);
			pos = strTemp.find('.', 0);
			if(pos != -1)
			{
				flagRight = 1;
				tableRight = strTemp.substr(0, pos);
				attrRight = strTemp.substr(pos+1, strTemp.length()-1);				
			}
			else{
				attrRight.clear();
				attrRight.insert(0,parsePointer ->left->left->right->value);
			}
			if(flagLeft)
			{
				vectorLeft.push_back((S.relationMap.find(tableLeft)->second).numTuples); // get the total number of tuples in that relation
				vectorLeft.push_back(S.relationMap.find(tableLeft)->second.attList.find(attrLeft)->second); //no of dist values of that attr
			}
			else
			{
				vectorLeft = S.GetNumTuples(relNames, attrLeft, numToJoin);
				tableLeft = S.GetTableName(attrLeft,relNames, numToJoin);
			}
			if(flagRight)
			{
				vectorRight.push_back((S.relationMap.find(tableRight)->second).numTuples);// get the total number of tuples in that relation
				vectorRight.push_back(S.relationMap.find(tableRight)->second.attList.find(attrRight)->second);//no of dist values of that attr
			}
			else
			{
				vectorRight = S.GetNumTuples(relNames, attrRight, numToJoin);
				tableRight = S.GetTableName(attrRight,relNames,numToJoin);
			}

			JoinNoTuples = calculateTupleNum(vectorLeft[0], vectorLeft[1], vectorRight[0], vectorRight[1], opCode);
						
			////////////////
			total_sum += JoinNoTuples;

			joinTableNames.push_back(tableLeft);joinTableCounter++;//]=tableLeft;
			joinTableNames.push_back(tableRight);joinTableCounter++;//]=tableRight;
			S.relationMap.find(tableLeft)->second.numTuples = JoinNoTuples;
			S.relationMap.find(tableRight)->second.numTuples = JoinNoTuples;
			
			relmapPointer  = S.relationMap.find(tableRight);

			// Get the minimum distint tuple count from the common join attribute
			double themin = min( S.relationMap.find(tableLeft)->second.attList.find(attrLeft)->second,
						S.relationMap.find(tableRight)->second.attList.find(attrRight)->second);
						
			// Apply minimum to both, just to be sure
			S.relationMap.find(tableRight)->second.attList.find(attrRight)->second = themin;
			
			for ( attmapPointer=relmapPointer->second.attList.begin() ; attmapPointer != relmapPointer->second.attList.end(); ++attmapPointer)
				S.relationMap.find(tableLeft)->second.attList.insert(pair<string, int>(attmapPointer->first,attmapPointer->second));
			

			S.relationMap.find(tableRight)->second.attList.clear();
			S.relationMap.find(tableRight)->second.attList = S.relationMap.find(tableLeft)->second.attList;
			
			vectorLeft.clear();
			vectorRight.clear();
		}
		else
		{
			flagLeft = 0;
			flagRight = 0;
			strTemp.clear();
			//JoinNoTuples = 0.0;
			isDifferent= true;
			strTemp.insert(0,parsePointer ->left->left->left->value);
			int pos = strTemp.find('.', 0);
			if(pos != -1)
			{
				flagLeft= 1;
				tableLeft.clear();
				tableLeft = strTemp.substr(0, pos);
				attrLeft = strTemp.substr(pos+1, strTemp.length()-1);				
			}
			else{
				attrLeft.clear();
				attrLeft.insert(0,parsePointer ->left->left->left->value);
			}


			if(flagLeft)
			{
				vectorLeft.push_back(S.relationMap.find(tableLeft)->second.numTuples); // get the total number of tuples in that relation
				vectorLeft.push_back(S.relationMap.find(tableLeft)->second.attList.find(attrLeft)->second); //no of dist values of that attr
			}
			else
			{
				vectorLeft = S.GetNumTuples(relNames, attrLeft, numToJoin);
				tableLeft = S.GetTableName(attrLeft,relNames, numToJoin);
			}

			//cout<<parsePointer ->left->left->left->value<<" "<<parsePointer ->left->left->right->value<<endl;
			//cout<<" vector left [0] = "<<vectorLeft[0];
			//cout<<" vector left [1] = "<<vectorLeft[1]<<endl;
			
			if(parsePointer->left->rightOr == NULL)
			{
				sum = S.calculateTupleNum(vectorLeft[0], vectorLeft[1], opCode);
				orSum.push_back(sum);
				total_sum += sum;
				//cout << "sum: " << sum << endl;
				vectorLeft.clear();	
			}
			else
			{
				if(opCode == EQUALS)
					orFactor.push_back(vectorLeft[1]);
				else
					orFactor.push_back(3.0);
				orAttrNames.push_back(attrLeft);

			}
		}
		
		
		
		
		
		
		double compositeFactor = 0.0;
		bool isComposite = false;
		if(parsePointer->left->rightOr)
		{
			struct OrList *traverseOR = parsePointer->left->rightOr;
		
			isComposite = true;
			while(traverseOR)
			{

				
		
				
				flagLeft = 0;
				flagRight = 0;
				strTemp.clear();
				strTemp.insert(0,traverseOR->left->left->value);
				int pos = strTemp.find('.', 0);
				if(pos != -1)
				{
					flagLeft= 1;
					tableLeft.clear();
					tableLeft = strTemp.substr(0, pos);
					attrLeft.clear();
					attrLeft.insert(0,strTemp.substr(pos+1, strTemp.length()-1));
				}
				else{
					attrLeft.clear();
					attrLeft.insert(0,traverseOR->left->left->value);
				}

				if(flagLeft){
					vectorLeft.push_back((S.relationMap.find(tableLeft)->second).numTuples); // get the total number of tuples in that relation
					vectorLeft.push_back(S.relationMap.find(tableLeft)->second.attList.find(attrLeft)->second); //no of dist values of that attr
				}
				else{
					vectorLeft.clear();
					tableLeft.clear();
					vectorLeft = S.GetNumTuples(relNames, attrLeft, numToJoin);
					tableLeft = S.GetTableName(attrLeft,relNames, numToJoin);
				}
				
				opCode = traverseOR->left->code;
				if(opCode == EQUALS)
					orFactor.push_back(vectorLeft[1]);
				else
					orFactor.push_back(3.0);
				orAttrNames.push_back(attrLeft);
				
				//cout<<parsePointer ->left->left->left->value<<" "<<parsePointer ->left->left->right->value<<endl;
				//cout<<" vector left [0] = "<<vectorLeft[0];
				//cout<<" vector left [1] = "<<vectorLeft[1]<<endl;
				sum = calculateTupleNum(vectorLeft[0], vectorLeft[1], opCode);
				orSum.push_back(sum);
				
				total_sum += sum;
				vectorLeft.clear();		
				traverseOR=traverseOR->rightOr;
				
				
			}


			if(orAttrNames.size() == 3)
			{
				//cout<<"Inside orAttrNames = 3"<<endl;
				if(orAttrNames[0] != orAttrNames[1] && orAttrNames[1] != orAttrNames[2] && orAttrNames[0] != orAttrNames[2] )
				{
		
					double m1 = orSum[0];
					double m2 = orSum[1];
					double m3 = orSum[2];
					double n = vectorLeft[0];
					sum =  n * (1.0-(1.0-m1/n)*(1.0-m2/n)*(1.0-m3/n));
				}
				else if(orAttrNames[0] == orAttrNames[1] && orAttrNames[1] == orAttrNames[2] && orAttrNames[0] == orAttrNames[2] )
				{
					double n = vectorLeft[0];
					sum =  n * (1/orFactor[0] + 1/orFactor[1] + 1/orFactor[2]);
	
				}
			}
			else if(orAttrNames.size() == 2)
			{
				//cout<<"Inside orAttrNames = 2::: orAttrNames = "<<orAttrNames[0]<<"   orAttrNames[1] = "<<orAttrNames[1]<<endl;
				if(orAttrNames[0] != orAttrNames[1])
				{
					//cout<<"Names not equal"<<endl;
					double m1 , m2;
					m1= orSum[0];
					m2= orSum[1];
					double n = vectorLeft[0];
					//cout<<"m1 = "<<m1<<"  m2 = "<<m2<<"   n = "<<n<<endl;
					sum = n * (1.0-(1.0-m1/n)*(1.0-m2/n));
						
				}
				else 
				{
					// the attributes are the same
					double n = vectorLeft[0];
					//cout<<"Factors are : orFactor[0] = " <<orFactor[0]<<"  orFactor[1] = "<<orFactor[1]<<"   n="<<n<<endl;
					sum = n *( (1/orFactor[0]) + (1/orFactor[1]));
				}
			}
		}
		if(sum > 0.0)
		{
			/*cout<<"Write out the changes in the numbers "<<endl;
			S.relationMap.find(tableLeft)->second.numTuples = sum;
			
			if(joinFlag == 1)
			{
				for(int z=0; z<joinTableCounter; z++)
					S.relationMap.find(joinTableNames[z].c_str())->second.numTuples = sum;
			}
*/
			
			
		andRes.push_back(sum);
			
		}
		else
			andRes.push_back(JoinNoTuples);

		if(andRes.size() == 2 &&  parsePointer->rightAnd != NULL)
		{

			double totalTuples = S.relationMap.find(tableLeft)->second.numTuples;
			//cout<<"Total tuples = "<<totalTuples<<endl;
			double totalFactors = 2 ;

			for(int i = 0 ; i < andRes.size();i++)
				totalFactors *= andRes[i];
			double answer = totalFactors/totalTuples;
			//cout<<"answer = "<<answer<<endl;
			
			//cout<<"Write out the changes in the numbers "<<endl;
			S.relationMap.find(tableLeft)->second.numTuples = answer;
			
			if(joinFlag == 1)
			{
				for(int z=0; z<joinTableCounter; z++)
					S.relationMap.find(joinTableNames[z].c_str())->second.numTuples = answer;
			}
		andRes.clear();

		}
		parsePointer = parsePointer->rightAnd;
	}
	
	//if(sum == 0){
	if(isDifferent == false){
		//cout << "Sum == 0 \n";
		//return JoinNoTuples;
			return total_sum;
	}
	else{
	
		
	
		//printf("-------------------------------\n");
		//printf("andRes: ");
		for(int i = 0 ; i < andRes.size();i++)
		{
			//cout << andRes[i] << "," ;
		}// printf("\n");
		
		//printf("andComposite: ");
		for(int i = 0 ; i < andComposite.size();i++)
		{
			//cout << andComposite[i] << ",";
		} //printf("\n");
		
		//printf("andFactors: ");
		for(int i = 0 ; i < andFactors.size();i++)
		{
			//cout << andFactors[i] << ",";
		}// printf("\n");
		//printf("-------------------------------\n");
	/*	
		// What now??
		double min = andRes[0];
		double total = 0.0;
		for(int i = 1 ; i < andRes.size();i++)
		{
			//andRes[i]
			if(andRes[i] < min)
				min = andRes[i];
		}
		return min;
		*/
		if(andRes.size() == 1) return andRes[0];
		double totalTuples = S.relationMap.find(tableLeft)->second.numTuples;
		//cout<<"Total tuples = "<<totalTuples<<endl;
		double totalFactors = 1 ;
		for(int i = 0 ; i < andRes.size();i++)
			totalFactors *= andRes[i];
		double answer = totalFactors/totalTuples;
		//cout<<"answer = "<<answer<<endl;	
		//return answer;
		return total_sum;
	}
	
}


void Statistics::init()
{
	char *relName[] = {"nation", "region", "part", "customer", "lineitem", "orders", "supplier","partsupp"};
	
	//nation
	AddRel(relName[0],25);	
	AddAtt(relName[0], "n_nationkey",25);
	AddAtt(relName[0], "n_regionkey",5);
	AddAtt(relName[0], "n_name",25);
	
	
	//region
	AddRel(relName[1],5);
	AddAtt(relName[1], "r_regionkey",5);
	AddAtt(relName[1], "r_name",5);
	
	//part
	AddRel(relName[2],200000);
	AddAtt(relName[2], "p_partkey",200000);
	AddAtt(relName[2], "p_size",50);
	AddAtt(relName[2], "p_name", 199996);
	AddAtt(relName[2], "p_container",40);
	
	//customer
	AddRel(relName[3],150000);
	AddAtt(relName[3], "c_custkey",150000);
	AddAtt(relName[3], "c_nationkey",25);
	AddAtt(relName[3], "c_mktsegment",5);
	AddAtt(relName[3], "c_name",333);
	
	//lineitem
	AddRel(relName[4],6001215);
	AddAtt(relName[4], "l_returnflag",3);
	AddAtt(relName[4], "l_discount",11);
	AddAtt(relName[4], "l_shipmode",7);
	AddAtt(relName[4], "l_orderkey",1500000);
	AddAtt(relName[4], "l_receiptdate",1500000);
	AddAtt(relName[4], "l_partkey",200000);
	AddAtt(relName[4], "l_shipinstruct",4);
	AddAtt(relName[4], "l_quantity",7655);
	
	//orders
	AddRel(relName[5],1500000);
	AddAtt(relName[5], "o_orderkey",1500000);
	AddAtt(relName[5], "o_custkey",150000);
	AddAtt(relName[5], "o_orderdate",150000);
	
	//supplier
	AddRel(relName[6],10000);
	AddAtt(relName[6], "s_suppkey",10000);
	AddAtt(relName[6], "s_nationkey",25);
	AddAtt(relName[6], "s_acctbal",765);
	
	//partsupp
	AddRel(relName[7],800000);
	AddAtt(relName[7], "ps_suppkey", 10000);
	AddAtt(relName[7], "ps_partkey", 200000);
	
//	CopyRel(relName[2], "p");
		

}


