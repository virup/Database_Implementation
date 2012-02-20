#include "QueryPlan.h"


QueryPlan::QueryPlan()
{
	
	
}
void inOrderEnum(QueryTreeNode *t, int level)
{
	if(t==NULL)
	{
		return;
	}
	inOrderEnum(t->left, level + 1 );
	cout<<" *****************"<<endl;
	cout<<" Level = "<<level<<endl;
	t->print();
	inOrderEnum(t->right, level + 1 );
}


char *QueryPlan::GetNameWithoutTable(char* name)
{
	string strTemp;
	strTemp.insert(0,name);
	int pos = strTemp.find('.', 0);
	if(pos != -1)
	{
		char *chTemp = new char[20];
		strcpy(chTemp,strTemp.substr(pos+1, strTemp.length()-1).c_str());	
		return chTemp;			
	}
	else
	{
		return name;
	}


}



int QueryPlan::checkAttPresent(struct AndList *parseTree, Schema *leftSchema,Schema *rightSchema)
{
	

	for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) 
	{
		if (parseTree == NULL) 	break;

		struct OrList *myOr = parseTree->left;
		for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) 
		{
			if (myOr == NULL) break;

			string strTemp;
			strTemp.clear();
			char * value = new char [20];
			strTemp.insert(0,myOr->left->left->value);
			int pos = strTemp.find('.', 0);
			if(pos != -1)
			{
				strcpy(value, strTemp.substr(pos+1, strTemp.length()-1).c_str());				
			}
			else
				strcpy(value,strTemp.c_str());

			if (myOr->left->left->code == NAME) 
			{
				if (leftSchema->Find (value) != -1) return 1; 
				else if (rightSchema->Find (value) != -1) return 1;
                                else{	cout << "ERROR: Could not find attribute1 " <<myOr->left->left->value << "\n";
					return 0;	
					}
			}
			
			strTemp.clear();
			value = new char [20];
			strTemp.insert(0,myOr->left->right->value);
			pos = strTemp.find('.', 0);
			if(pos != -1)
			{
				strcpy(value, strTemp.substr(pos+1, strTemp.length()-1).c_str());				
			}
			else
			{
				strcpy(value,strTemp.c_str());
			}
		
			if (myOr->left->right->code == NAME) 
			{
				if (leftSchema->Find (value) != -1)return 1;
				else if (rightSchema->Find (value) != -1)return 1;
				else {
					cout << "ERROR: Could not find attribute2 " << myOr->left->right->value << "\n";
					return 0;
				}
			}
		}	
	}
}


void QueryTreeNode::print()
{
	switch(opType)
	{
	
	case  SELECT:
			cout<<"*********************";
			cout<<"Select File Operation"<<endl;
			//cout<<"Table = "<<table1<<endl;
			
			cout<<"Input Pipe ID "<<leftPipeID<<endl;
			cout<<"Out Pipe ID"<<outputPipeID<<endl;
			cout<<"Select File CNF: "<<endl;
			if(cnf != NULL)
			{	
				cnf->Print();
			}
			cout<<"*********************";
		break;
	case SELECT_PIPE:
			
		cout<<"Select Pipe"<<endl;
		if(cnf != NULL)
			cnf->Print();
		cout<<endl;
		cout<<"In Pipe "<<leftPipeID<<endl;
		cout<<"Out Pipe "<<outputPipeID<<endl;
	break;
	
	case  PROJECT:
		cout<<"*********************";
		cout<<"Project Operation "<<table1<<endl;
		cout<<"Input Pipe ID "<<leftPipeID<<endl;
		cout<<"Output Pipe ID "<<outputPipeID<<endl;
		cout<<"Keep Me"<<endl;
		for(int jj = 0; jj < numAttsOutput; jj++)
		{
			cout<<KeepMe[jj]<<" ";
		}
		cout<<endl;
		/*cout<<"NumAttsInput"<<numAttsInput<<endl;
		cout<<"NumAttsOutput"<<numAttsOutput<<endl; 
		*/
		cout<<"*********************"<<endl;;
		break;

	case JOIN:
		cout<<"************"<<endl;
		cout<<"Join Operation"<<endl;
		cout<<"Input pipe ID "<<leftPipeID<<endl;
		cout<<"Input pipe ID "<<rightPipeID<<endl;
		cout<<"Output pipe ID "<<outputPipeID<<endl;
		
			
		cout<<"Join CNF:"<<endl;
		cnf->Print();
		cout<<"*********************"<<endl;
		break;

	case SUM :
		cout<<"*********************";
		cout<<"Sum Operation"<<endl;
		cout<<"Input Pipe ID "<<leftPipeID<<endl;
		cout<<"Output Pipe ID "<<outputPipeID<<endl;
		cout<<"Function = ";function->Print();
	cout<<"*********************"<<endl;
			break;

	case GROUPBY:
			cout<<"*********************"<<endl;
			cout<<"Group By"<<endl;
			cout<<"Input Pipe ID "<<leftPipeID<<endl;
			cout<<"Output Pipe ID "<<outputPipeID<<endl;
			cout<<"Order maker = ";
			orderMaker->Print();
			cout<<"Function = ";
			function->Print();
			cout<<"*********************"<<endl;
		break;

	case DUPLICATE_REMOVAL:
		cout<<"*********************"<<endl;
		cout<<"Duplicate Removal"<<endl;
		cout<<"Input Pipe ID "<<leftPipeID<<endl;
		cout<<"Output Pipe ID "<<outputPipeID<<endl;
		cout<<"*********************"<<endl;
	break;
	
	default:
		cout<<"None"<<endl;
		}
}

const char * QueryPlan::GetRealName(const char * name)
{
	TableList *pointer = tables;
	while(pointer)
	{
		if(strcmp(pointer->tableName,name) == 0 || strcmp(pointer->aliasAs, name) == 0)
			return pointer->aliasAs;
		
		pointer = pointer->next;

	}
}
vector<string> QueryPlan::getAtts(AndList *b, vector<string> rel) {
	char *c1 = new char[15];

	AndList *tempspans = b;

	vector<string> tempAtts;

	for (int ii = 0; ii < rel.size(); ii++) 
	{
		strcpy(c1, GetRealName(rel[ii].c_str()));
		Schema sch("catalog", c1);
		tempspans = b;
		while (tempspans) 
		{
			if (tempspans->left->left->left) 
			{
				if (sch.Find(tempspans->left->left->left->value) != -1) 
				{
					string s(tempspans->left->left->left->value);
					tempAtts.push_back(s);
				}
			}
			if (tempspans->left->left->right->code == NAME) 
			{
				if (sch.Find(tempspans->left->left->right->value) != -1) 
				{
					string s(tempspans->left->left->right->value);
					tempAtts.push_back(s);
				}
			}
			if (tempspans->left->rightOr) 
			{
				struct OrList *traverseOR = tempspans->left->rightOr;

				while (traverseOR) 
				{

					if (sch.Find(traverseOR->left->left->value) != -1) 
					{
						// attr found
						string s(traverseOR->left->left->value);
						tempAtts.push_back(s);
					}
					if (traverseOR->left->right->code == NAME) 
					{ // right side of =
						if (sch.Find(traverseOR->left->right->value) != -1) 
						{
							// attr found
							string s(traverseOR->left->left->value);
							tempAtts.push_back(s);
						}
					}
					traverseOR=traverseOR->rightOr;
				}

				
			}
			tempspans = tempspans->rightAnd;
		}

	}
	return tempAtts;
}

AndList* QueryPlan::seperateAndList(AndList* boolean)
{
	Operand *o1 = new Operand;
	Operand *o2 = new Operand;
	o1->code = boolean->left->left->left->code;
	o2->code = boolean->left->left->right->code;
	
	o1->value = boolean->left->left->left->value;
	o2->value = boolean->left->left->right->value;
	
	ComparisonOp *cOp = new ComparisonOp;
	cOp->left = o1;
	cOp->right = o2;
	cOp->code = boolean->left->left->code;
	
	OrList *ol = new OrList;
	ol->left = cOp;
	ol->rightOr = NULL;
	
	AndList *an = new AndList;
	an->left = ol;
	an->rightAnd = NULL;
	
	OrList *temp = ol;
	if (boolean->left->rightOr) {
		struct OrList *traverseOR = boolean->left->rightOr;

		while (traverseOR) {
			Operand *o11 = new Operand;
			Operand *o22 = new Operand;
			o11->code = traverseOR->left->left->code;
			o22->code = traverseOR->left->right->code;

			o11->value = traverseOR->left->left->value;
			o22->value = traverseOR->left->right->value;

			ComparisonOp *cOp1 = new ComparisonOp;
			cOp1->left = o11;
			cOp1->right = o22;
			cOp1->code = traverseOR->left->code;

			
			OrList *ol1 = new OrList;
			ol1->left = cOp1;
			ol1->rightOr = NULL;
			temp->rightOr = ol1;
			temp = temp->rightOr;
			
			traverseOR = traverseOR->rightOr; 
			
		}
	}
	return an;
}

vector<string> QueryPlan::getCommonAtts(vector<string> Att, vector<string> rel)
{
	char *c1 = new char[15];
	char *c2 = new char[15];
	
	vector<string> tempAtts;
	
	for(int ii = 0; ii < rel.size(); ii++)
	{
		strcpy(c1, GetRealName(rel[ii].c_str()));
		Schema sch("catalog", c1);
		for(int jj = 0; jj < Att.size(); jj++)
		{
			strcpy(c2, Att[jj].c_str());
			if (sch.Find(GetNameWithoutTable(c2)) != -1) 
			{
				// attr found
				tempAtts.push_back(Att[jj]);
				
			}
		}
	}
	
	return tempAtts;
}


	
vector<string> QueryPlan::unionAttributes(vector<string> v1, vector<string> v2)
{
	vector<string> newAtts;
	if(v1.size()>0){
		newAtts.push_back(v1[0]);
	}
	for(int jj = 0; jj < v1.size(); jj++)
	{
		int flag = 0;
		for(int kk = 0; kk < newAtts.size(); kk++)
		{
			if(newAtts[kk].compare(v1[jj])==0)
			{
				flag = 1;
				break;
			}
		}
		if(flag == 0){
			newAtts.push_back(v1[jj]);
		}
	}
	if(newAtts.size() == 0 && v2.size()>0){
		newAtts.push_back(v2[0]);
	}
	
	for(int jj = 0; jj < v2.size(); jj++)
	{
		int flag = 0;
		for(int kk = 0; kk < newAtts.size(); kk++)
		{
			if(newAtts[kk].compare(v2[jj])==0)
			{
				flag = 1;
				break;
			}
		}
		if(flag == 0){
			newAtts.push_back(v2[jj]);
		}
			
	}
	
	
	return newAtts;
}

Schema *QueryPlan::createSchema(QueryTreeNode *q)
{
	if(q == NULL)
		cout<<" NULL q ! "<<endl;;
	cout<<"opType = "<<q->opType<<endl;
	if(q->opType == SELECT || q->opType == SELECT_PIPE)
		return q->schema;
	if(q->opType == PROJECT)
	{
		Attribute *att = new Attribute[q->numAttsOutput];
		//Schema tempSchema("catalog", q->table1.c_str());
		for(int i = 0 ; i < q->numAttsOutput; i++)
		{
			att[i].name = q->schema->myAtts[q->KeepMe[i]].name;
			att[i].myType  = q->schema->myAtts[q->KeepMe[i]].myType;
		}
		
		for(int i = 0 ; i < q->numAttsOutput; i++)
		{
			//cout<<" "<<att[i].name << "   "<<att[i].myType<<endl;
		}
		Schema *newSchema = new Schema("In-mem-Schema",q->numAttsOutput,att);		
		return newSchema;
	}
	if(q->opType == JOIN)
	{
		/*char *chtemp = new char [20];
		cout<<"table1 = "<<q->table1.c_str()<<endl;
		cout<<"table2 = "<<q->table2.c_str()<<endl;
		strcpy(chtemp,GetRealName(q->table1.c_str()));
		Schema sc1("catalog", chtemp);
		
		strcpy(chtemp,GetRealName(q->table2.c_str()));
		cout<<"chtemp2 = "<<chtemp<<endl;
		Schema sc2("catalog", chtemp);
		cout<<" made the schema"<<endl;
		int numAttsOutput = sc1.GetNumAtts() + sc2.GetNumAtts();
		cout<<"total = "<<numAttsOutput<<endl;
		Attribute *att = new Attribute[numAttsOutput];
		for(int i = 0 ; i < sc1.GetNumAtts(); i++)
		{
			att[i].name = sc1.myAtts[i].name;
			att[i].myType  = sc1.myAtts[i].myType;
		}
		int sc1No = sc1.GetNumAtts();
		cout<<"sc1 no = "<<sc1No<<endl;
		for(int i = 0 ; i < sc2.GetNumAtts(); i++)
		{
			att[i+sc1No].name = sc2.myAtts[i].name;
			att[i+sc1No].myType  = sc2.myAtts[i].myType;
		}
	
		for(int i = 0 ; i < numAttsOutput; i++)
		{
			cout<<" "<<att[i].name << "   "<<att[i].myType<<endl;
		}
		Schema *newSchema = new Schema("In-mem-Schema",numAttsOutput,att);		
		return newSchema;
		*/
		return q->schema;

	}
	if(q->opType == SUM || q->opType == GROUPBY || q->opType == DUPLICATE_REMOVAL)
		return q->schema;
	

}

AndList *QueryPlan::makeAndListAll(Schema *sc)
{
	Attribute a = sc->myAtts[0];
	cout<<" *********MAKE ANDLIST ==== "<<a.myType<<endl;
	Operand *o1 = new Operand;
	o1->value = new char[90];
	Operand *o2 = new Operand;
	o2->value = new char[90];
	cout<<"Done till here"<<endl;
	if(a.myType == Int)
		o1->code = o2->code = INT;
	else if(a.myType == Double)
		o1->code = o2->code = DOUBLE;
	else
		o1->code = o2->code = STRING;
	
	cout<<"Done till here2"<<endl;
	strcpy(o1->value, a.name);
	strcpy(o2->value, a.name);
	cout<<"String copy done"<<endl;
	ComparisonOp *cOp = new ComparisonOp;
	cOp->left = o1;
	cOp->right = o2;
	cOp->code = EQUALS;
	
	OrList *ol = new OrList;
	ol->left = cOp;
	ol->rightOr = NULL;
	
	AndList *an = new AndList;
	an->left = ol;
	an->rightAnd = NULL;
	
	cout<<"Returning"<<endl;
	return an;
}


 int currentPipeID;


QueryTreeNode* QueryPlan::enumerate(vector<string> Rels, vector<string> Atts, AndList *boolean, Statistics &stats, int level) 
{

	cout<<" Inside this enumerate" <<endl;
	
	
	/*cout<<" Arrtibutes size = "<<Atts.size()<<endl;
	for(int jj = 0; jj < Atts.size(); jj++)
	{
		cout<<Atts[jj]<<endl;
	}
	*/
	cout<<endl;
	cout<<"Relation size = "<<Rels.size()<<endl;
	cout<<" Rels are = "<<endl;
	for(int jj = 0; jj < Rels.size(); jj++)
	{
		cout<<Rels[jj]<<endl;
	}
	cout<<"AndList = NULL ? ";
	if(boolean == NULL)
		cout<<"True"<<endl;
	else
		cout<<"False"<<endl;
	if (Rels.size()==0 || Rels.size()==1)
	{
		
		cout<< "Inside first if " <<endl;
		if(Rels.size()==1 && boolean)
		{
			
			char *c = new char[15];
			strcpy(c, Rels[0].c_str());
	
			double cost = stats.Estimate(boolean, &c, 1);
			cout<<"Cost "<<cost<<endl;
			stats.Apply(boolean, &c, 1);

			char *zz = new char[15];
			strcpy(zz, GetRealName(Rels[0].c_str()));
			Schema sc("catalog", zz);
			
			cout<<"SELECT QUERY NODE"<<endl;
			QueryTreeNode *newQueryNode = new QueryTreeNode(SELECT);
			newQueryNode->table1 = Rels[0];
			newQueryNode->cost = cost;
			
			newQueryNode->cnf = new CNF();
			newQueryNode->cnf->GrowFromParseTree(boolean, &sc, newQueryNode->record);
			newQueryNode->leftPipeID= currentPipeID++;
			newQueryNode->outputPipeID = currentPipeID++;
			newQueryNode->schema = new Schema("catalog", zz);
			
			newQueryNode->left = NULL;
			newQueryNode->right = NULL;
			
			if(Atts.size())
			{
				cout<<"SELEFT FILE WE WANT"<<endl;
				QueryTreeNode *newProjectQueryNode = new QueryTreeNode(PROJECT);
				newProjectQueryNode->table1 = Rels[0];
				newProjectQueryNode->cost = 0;
				newProjectQueryNode->leftPipeID = newQueryNode->outputPipeID;
				newProjectQueryNode->outputPipeID = currentPipeID++;
				newProjectQueryNode->left = newQueryNode;
				newProjectQueryNode->right = NULL;
				
				char *qw = new char[15];
				strcpy(qw, GetRealName(Rels[0].c_str()));
				
				Schema sche("catalog", qw);
				newProjectQueryNode->numAttsInput = sche.numAtts;
				//int num = newProjectQueryNode->numAttsInput;
				newProjectQueryNode->KeepMe = new int[Atts.size()];
				newProjectQueryNode->schema = new Schema("catalog", qw);
				int count = 0;


				int pos;
				for(int jj = 0; jj < Atts.size(); jj++)
				{
					
					pos = Atts[jj].find('.', 0);
					if(pos != -1)
					{
						strcpy(qw, Atts[jj].substr(pos+1, Atts[jj].length()-1).c_str());				
					}		
					else
						strcpy(qw, Atts[jj].c_str());



					if(sche.Find(qw) != -1)
					{
						newProjectQueryNode->KeepMe[count++] = sche.Find(qw);
					}
					
				}
				
				newProjectQueryNode->numAttsOutput = Atts.size();
				
				return newProjectQueryNode;
			}
			return newQueryNode;
		}

		if(Atts.size())
		{



///////////////////////////////////////////////////

			char *zz = new char[15];
			strcpy(zz, GetRealName(Rels[0].c_str()));
			Schema *sc = new Schema("catalog", zz);
			
			cout<<"SELECT QUERY NODE"<<endl;
			QueryTreeNode *newQueryNode = new QueryTreeNode(SELECT);
			newQueryNode->table1 = Rels[0];
			newQueryNode->cost = 0;
			
			newQueryNode->cnf = new CNF();
			
			newQueryNode->cnf->GrowFromParseTree(makeAndListAll(sc), sc, newQueryNode->record);
			newQueryNode->cnf->Print();
			newQueryNode->leftPipeID= currentPipeID++;
			newQueryNode->outputPipeID = currentPipeID++;
			newQueryNode->schema = new Schema("catalog", zz);
			
			newQueryNode->left = NULL;
			newQueryNode->right = NULL;

///////////////////////////////////



			cout<<"PROJECT QUERYNODE"<<endl;
			QueryTreeNode *newProjectQueryNode = new QueryTreeNode(PROJECT);
			newProjectQueryNode->table1 = Rels[0];
			newProjectQueryNode->cost = 0;
			newProjectQueryNode->leftPipeID = newQueryNode->outputPipeID;
			newProjectQueryNode->outputPipeID = currentPipeID++;
			newProjectQueryNode->left = newQueryNode;
			newProjectQueryNode->right = NULL;
			
			char *qw = new char[15];
			strcpy(qw, GetRealName(Rels[0].c_str()));
			
			Schema sche("catalog", qw);
			newProjectQueryNode->schema = new Schema("catalog",qw);
			newProjectQueryNode->numAttsInput = sche.numAtts;
			newProjectQueryNode->KeepMe = new int[Atts.size()];
			int count = 0;
			int pos ; 
			for (int jj = 0; jj < Atts.size(); jj++) 
			{
				pos = Atts[jj].find('.', 0);
					if(pos != -1)
					{
						strcpy(qw, Atts[jj].substr(pos+1, Atts[jj].length()-1).c_str());				
					}		
					else
						strcpy(qw, Atts[jj].c_str());
				if (sche.Find(qw) != -1) 
					newProjectQueryNode->KeepMe[count++] = sche.Find(qw);
				
			}

			newProjectQueryNode->numAttsOutput = Atts.size();		
			
			return newProjectQueryNode;
		}		
		cout<<"end"<<endl;

		cout<<"SELECT QUERY NODE 2"<<endl;
		QueryTreeNode *dn = new QueryTreeNode(SELECT);
		dn->table1 = Rels[0];
		dn->cost = stats.relationMap.find(Rels[0])->second.numTuples;
		dn->left = NULL;
		dn->right = NULL;
		char *relationName = new char[20];
		strcpy(relationName,GetRealName(Rels[0].c_str()));
		cout<<"Relation name = " <<relationName<<"|||"<<endl;
		dn->schema = new Schema("catalog",relationName);
		dn->cnf = new CNF();
		if(boolean == NULL)
			cout<<" Boolean == NULL "<<endl;
		dn->cnf->GrowFromParseTree(makeAndListAll(dn->schema), dn->schema, dn->record);
		//dn->cnf = NULL;
		
			dn->leftPipeID= currentPipeID++;
			dn->outputPipeID = currentPipeID++;
		
		return dn;
	
		
	}
	else
	{
		cout<<"In else"<<endl;
		QueryTreeNode *bestT = NULL;
		// intialize
		Statistics bestStats;
		double bestCost = -1.0;
		
		
		queue<RelationPlanWrapper> *qc = new queue<RelationPlanWrapper>();

		RelationPlanWrapper *qu = new RelationPlanWrapper();
		string s(Rels[0]);
		
		
		qu->v1.push_back(s);

		for (int i=1;; i++) 
		{
			if(i<Rels.size())
			{
				string ss(Rels[i]);
				qu->v2.push_back(ss);
			}
			else
				break;

		}
		qc->push(*qu);

		int count = 0;
		vector<string> relLCopy;
		vector<string> relRCopy;
		while (qc->size() > 0) 
		{
			for (int index=0; index < qc->front().v2.size(); index++) {
				
				RelationPlanWrapper *q = new RelationPlanWrapper();
				//cout<<"inside"<<endl;
				for (int i=0; i<qc->front().v1.size(); i++) {
					string s11(qc->front().v1[i]);
					q->v1.push_back(s11);
				}
				int cnt = 0;
				for (int i=0; i<qc->front().v2.size(); i++) {
					if (i==index)
						continue;
					cnt++;
					string s12(qc->front().v2[i]);
					q->v2.push_back(s12);
				}
				if (cnt>0) {
					string s13(qc->front().v2[index]);
					q->v1.push_back(s13);
					qc->push(*q);
				}

			}
									
			vector<string> relL;
			vector<string> relR;
			
			int j, relLCount = 0, relRCount = 0;
			char *str;
			for (j=0; j<qc->front().v1.size(); j++) {
				string s14(qc->front().v1[j]);
				relL.push_back(s14);
				relLCount++;
			}
			
			cout<<endl;

			for (j=0; j<qc->front().v2.size(); j++) 
			{
				string s15(qc->front().v2[j]);
				relR.push_back(s15);

				
				relRCount++;
			}
			cout<<"relLCount = "<<relLCount<<endl;
			cout<<"relRCount = "<<relRCount<<endl;
			
			// stats copy
			Statistics statsCopy(stats);
			
			////////////////////////////////////////////////////
			// splitting preds
			int ee = 0;
			struct AndList *traverse = boolean;
			AndList *spans = NULL;
			AndList *tempspans = NULL;
			AndList *onlyL = NULL;
			AndList *temponlyL = NULL;
			AndList *onlyR = NULL;
			AndList *temponlyR = NULL;
						
			while (traverse) 
			{
				// relL check
				bool relLFlag = false, relRFlag = false;
				for (int jj = 0; jj < relLCount; jj++) 
				{
					char *c1 = new char[15];
					strcpy(c1, GetRealName(relL[jj].c_str()));
					cout<<"Left relation = "<<c1<<endl;
					Schema sch("catalog", c1);
					
					if (sch.Find(GetNameWithoutTable(traverse->left->left->left->value)) != -1) 
					{
						// attr found
						cout<<"Left Attr found !  "<<traverse->left->left->left->value<<endl;
						relLFlag = true;
						break;
					}
					if (traverse->left->left->right->code == NAME) 
					{ // right side of =
						if (sch.Find(GetNameWithoutTable(traverse->left->left->right->value)) != -1) 
						{
							// attr found
							cout<<"Left Attr found !  "<<traverse->left->left->right->value<<endl;
							relLFlag = true;
							break;
						}
					}

					if (traverse->left->rightOr) 
					{
						struct OrList *traverseOR = traverse->left->rightOr;
						while (traverseOR) 
						{

							if (sch.Find(GetNameWithoutTable(traverseOR->left->left->value)) != -1) 
							{
								// attr found
								cout<<"Left Attr found !  "<<traverseOR->left->left->value<<endl;
								relLFlag = true;
								break;
							}
							if (traverseOR->left->right->code == NAME) 
							{ // right side of =
								if (sch.Find(GetNameWithoutTable(traverseOR->left->right->value))!= -1) 
								{
									// attr found
									cout<<"Left Attr found !  "<<traverseOR->left->right->value<<endl;
									relLFlag = true;
									break;
								}
							}
							traverseOR=traverseOR->rightOr;
						}
						if (relLFlag) 
						{
							break;
						}
					}
					//delete c1;
				}
				// relR check
				for (int jj = 0; jj < relRCount; jj++) 
				{
					char *c2 = new char[15];
					strcpy(c2, GetRealName(relR[jj].c_str()));
					cout<<"right relation : "<<c2<<endl;
					Schema sch("catalog", c2);
					
					if (sch.Find(GetNameWithoutTable(traverse->left->left->left->value)) != -1) 
					{
						// attr found
						cout<<"Right Attr found !  "<<traverse->left->left->left->value<<endl;
						relRFlag = true;
						break;
					}
					if (traverse->left->left->right->code == NAME) 
					{ // right side of =
						if (sch.Find(GetNameWithoutTable(traverse->left->left->right->value)) != -1) 
						{
							// attr found
							cout<<"Right Attr found !  "<<traverse->left->left->right->value<<endl;
							relRFlag = true;
							break;
						}
					}
					if (traverse->left->rightOr) 
					{
						struct OrList *traverseOR = traverse->left->rightOr;
						while (traverseOR) 
						{
							
							if (sch.Find(GetNameWithoutTable(traverseOR->left->left->value)) != -1) 
							{
								// attr found
								cout<<"Right Attr found !  "<<traverseOR->left->left->value<<endl;
								relRFlag = true;
								break;
							}
							if (traverseOR->left->right->code == NAME) 
							{ // right side of =
								if (sch.Find(GetNameWithoutTable(traverseOR->left->right->value))!= -1) 
								{
									// attr found
									cout<<"Right Attr found !  "<<traverseOR->left->right->value<<endl;
									relRFlag = true;
									break;
								}
							}
							traverseOR=traverseOR->rightOr;
						}
						if (relRFlag) 
						{
							break;
						}
					}
				}
				AndList *t = seperateAndList(traverse);
		
				if(relLFlag && relRFlag)
				{
					cout<<"-->>Both true"<<endl;
					if (spans == NULL) 
					{
						spans = tempspans = t;
					} 
					else 
					{
						tempspans->rightAnd = t;
						tempspans = tempspans->rightAnd;
					}
			
				}
				else if(relLFlag)
				{
					cout<<" Only relLFlag TRUE"<<endl;
					if(onlyL == NULL)
					{
						onlyL = temponlyL = t;
						
					}
					else
					{
						temponlyL->rightAnd = t;
						temponlyL = temponlyL->rightAnd;  
					}
				//	cout<<"In left "<<ee<<endl;
									
				}
				else if(relRFlag)
				{
					cout<<" Only relRFlag TRUE"<<endl;
					if (onlyR == NULL) 
					{
						onlyR = temponlyR = t;

					} 
					else 
					{
						temponlyR->rightAnd = t;
						temponlyR = temponlyR->rightAnd;
					}
					//cout<<"In right "<<ee<<endl;
				}
				else 
				{
					cout<<"no where"<<endl;
				}
					
				ee++;
				traverse = traverse->rightAnd;
			}
							 
		
		///////////////////////////////////////////////////
						 
		
			vector<string> LAtts = getAtts(spans, relL); // returns a list of attributes from span, which are there in the left relations
	       		 vector<string> temp = getCommonAtts(Atts, relL); // returns a list of attributes from Attr, which are there in the left relation
		   
		   
		   	cout<<endl<<endl<<endl<<"Left tree"<<endl;
			cout<<"Relations : "<<endl;			 
		 	for (int jj=0; jj < relL.size(); jj++)
				cout<<relL[jj]<<" ";
		   	cout<<"Size of left tree attribute = " <<unionAttributes(LAtts, temp).size()<<endl;
		   	QueryTreeNode *tnl = enumerate(relL, unionAttributes(LAtts, temp), onlyL, statsCopy, level+1);
		   	double costL; 
		 	cout<<"tnl->cost = "<<tnl->cost<<endl;
		 	  if(tnl->cost != -45678)
				   costL = tnl->cost;
		  	 else
				   costL = 0;
		   	cout<<"costL"<<costL<<endl;
		  
		   	//cout<<"after relL recursive call"<<endl;
			
		  	vector<string> RAtts = getAtts(spans, relR);
		  	vector<string> temp1 = getCommonAtts(Atts, relR);

			cout<<endl<<endl<<"right tree"<<endl;
			cout<<"Relations : "<<endl;	
			for (int jj=0; jj < relR.size(); jj++)
				cout<<relR[jj]<<" ";
			
			//cout<<endl<<"making call for relR"<<" count "<<relRCount<<endl;
			cout<<"Size of right tree attribute = " <<unionAttributes(RAtts, temp1).size()<<endl;
			QueryTreeNode *tnr = enumerate(relR, unionAttributes(RAtts, temp1), onlyR, statsCopy, level+1);
			double costR;
			cout<<"tnr->cost = "<<tnr->cost<<endl;
			if(tnr->cost != -45678)
			{
				costR = tnr->cost;
			}
			else
				costR = 0;
			cout<<"costR"<<costR<<endl;
			
			
			char ** relNames = (char**)malloc(Rels.size());
			
			for(int jj = 0; jj < Rels.size(); jj++)
			{
				char *cstr = new char[15];
				strcpy(cstr, Rels[jj].c_str());
				relNames[jj] = cstr;
			}
			
			if(spans)
			{
				cout<<"NO SPAN SSSSSSSSSSSSSSSSSSSSSSS"<<endl;
				cout<<spans->left->left->left->value<<endl;
				cout<<spans->left->left->right->value<<endl;
				tempspans = spans;
				while(tempspans)
				{
					if(tempspans->left->left->left)
					{
						cout<<endl<<"left "<<tempspans->left->left->left->value<<endl;
						cout<<tempspans->left->left->right->value<<endl;
					}
					
					tempspans = tempspans->rightAnd;
				}

						
			}
			//cout<<"after spans check"<<endl;
			double extraCost = 0;
			if(spans){
				 extraCost = statsCopy.Estimate(spans, relNames, Rels.size());
				 
			}
			
			cout<<"EXTRA : "<<extraCost<<endl;
			cout<<"costL:"<<costL<<endl;
			cout<<"costR:"<<costR<<endl;
			double currCost = extraCost + costL + costR;
			cout<<"CURRENT cost =======>"<<currCost<<"   level: "<<level<<endl;

			if(currCost < bestCost || bestCost < 0)
			{
				
				
				
				relLCopy.clear();
				relLCopy = relL;
				
				relRCopy.clear();
				relRCopy = relR;
				
				statsCopy.Apply(spans, relNames, Rels.size());
				bestStats = statsCopy;
				
				QueryTreeNode *jn = new QueryTreeNode(JOIN);
				jn->table1 = tnl->table1;
				jn->table2 = tnr->table1;
					
				
				jn->leftPipeID = tnl->outputPipeID;
				jn->rightPipeID = tnr->outputPipeID;
				jn->outputPipeID = currentPipeID++;
				jn->left = tnl;
				jn->right = tnr;
				jn->cost = bestCost;

				Schema *leftSchema = createSchema(tnl);cout<<"Schemas created left"<<endl;
				Schema *rightSchema = createSchema(tnr);cout<<"Schemas created right"<<endl;
				cout<<"Left schema attr no = "<<leftSchema->GetNumAtts()<<endl;
				cout<<"Right schema attr no = "<<rightSchema->GetNumAtts()<<endl;				
				
				
				char *chtemp = new char [20];
				
		
				
				int numAttsOutput = leftSchema->GetNumAtts() + rightSchema->GetNumAtts();
				cout<<"total = "<<numAttsOutput<<endl;
				Attribute *att = new Attribute[numAttsOutput];
				for(int i = 0 ; i < leftSchema->GetNumAtts(); i++)
				{
					att[i].name = leftSchema->myAtts[i].name;
					att[i].myType  = leftSchema->myAtts[i].myType;
				}
				int sc1No = leftSchema->GetNumAtts();
				for(int i = 0 ; i < rightSchema->GetNumAtts(); i++)
				{
					att[i+sc1No].name = rightSchema->myAtts[i].name;
					att[i+sc1No].myType  = rightSchema->myAtts[i].myType;
				}
	
				for(int i = 0 ; i < numAttsOutput; i++)
				{
					cout<<" "<<att[i].name << "   "<<att[i].myType<<endl;
				}
				jn->schema = new Schema("newJoinSchema",numAttsOutput,att);

				/*int test = checkAttPresent(spans, leftSchema, rightSchema);
				cout<<" Test = " << test << endl;
				if(test == 1)
				*/
				{
					cout<<"Going into GrowFromParseTree"<<endl;
					jn->cnf = new CNF();
					jn->cnf->GrowFromParseTree(spans, leftSchema, rightSchema, jn->record);
				
					bestT = jn;
					cout<<"INSIDE best cost change"<<endl;
					//cout<<"BESTCOST ======>"<<bestCost<<endl;
					bestCost = currCost;
				}
				/*else
				{
					cout<<" Attribute not found .. tree not created";
					jn->cost = 99999999999999999999999.0;
					bestT = jn;
					//qc->pop();
					//continue;
				}*/
				
								
				
					
			}
			else
			{
			
			}

			cout<<endl;
			cout<<endl;
			qc->pop();

			cout<<"end of while"<<endl;
		}
		
		
		stats = bestStats;
		//cout<<"BIGGG@@@@@@@@@@@@@@@@@@@@@@@"<<bestCost<<endl;
		
		/*cout<<"RelLCopy"<<endl;
		
		for(int jj = 0; jj < relLCopy.size(); jj++)
		{
			cout<<relLCopy[jj]<<" ";
		}
		
		cout<<"RelRCopy"<<endl;
		
		for(int jj = 0; jj < relRCopy.size(); jj++)
		{
			cout<<relRCopy[jj]<<" ";
		}
		*/
		//return bestCost;
		return bestT;
	}
	
	

}

QueryPlan::~QueryPlan()
{
}






// Query Tree Node
QueryTreeNode :: QueryTreeNode (){
	opType = NONE;
	valid = true;
	leftPipeID = 0;
	rightPipeID = 0;
	schema = NULL; //schema for output
	cnf = NULL; // select, select pipe, join
	orderMaker = NULL; // group by
	function = NULL; // sum, group by
	left = NULL; // child
	right = NULL; // child
	KeepMe = NULL;
	numAttsInput = -1;
	numAttsOutput = -1;
	cost = -1.0;
}

QueryTreeNode :: QueryTreeNode (Operation _opType) : opType(_opType){
	//opType = NONE;
	valid = true;
	leftPipeID = 0;
	rightPipeID = 0;
	schema = NULL; //schema for output
	cnf = NULL; // select, select pipe, join
	orderMaker = NULL; // group by
	function = NULL; // sum, group by
	left = NULL; // child
	right = NULL; // child
	KeepMe = NULL;
	numAttsInput = -1;
	numAttsOutput = -1;
	cost = -1.0;
}



