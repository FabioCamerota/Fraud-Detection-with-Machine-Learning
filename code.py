from neo4j import GraphDatabase
import matplotlib.pyplot as plt
import numpy as np
import random

#access point to Neo4j local DBMS
driver = GraphDatabase.driver(
  "bolt://localhost:11003")

#useful function for debugging purpose
def print__ten_transactions():
    cypher_query = '''
        MATCH (t:Transaction)
        RETURN t.id
        LIMIT 10
    '''
    with driver.session(database="neo4j") as session:
        results = session.read_transaction(
            lambda tx: tx.run(cypher_query).data())
    for record in results:
        print(record)

#removes one random transaction
def remove_one_transaction():
    cypher_query_2 = '''
        MATCH (t:Transaction)
        WHERE (t.id = $tx_id)
        DETACH DELETE t
    '''
    #transaction_id is 'tx-number' starting from number=1
    tx_id = "tx-"+str(random.randint(1,323489))
    with driver.session(database="neo4j") as session:
        results = session.write_transaction(
            lambda tx: tx.run(cypher_query_2,tx_id=tx_id).data())

TOT_TRANSACTIONS = 323489
#remove x% transactions
def remove_transactions():
    percentage_transactions = TOT_TRANSACTIONS * 0.05 # remove 5% transactions
    for i in range(0,int(percentage_transactions)):
        remove_one_transaction()

#There are more labels than nodes, n labels = 656895
#count number of labels and make charts
def count_labels():

    cypher_query = '''
    CALL db.labels() YIELD label
    CALL apoc.cypher.run('MATCH (:`'+label+'`) RETURN count(*) as count', {})
    YIELD value
    RETURN label as Label, value.count AS Count
    '''

    with driver.session(database="neo4j") as session:
        results = session.read_transaction(
            lambda tx: tx.run(cypher_query).data())
        
        labels, count = [], []
        for record in results:
            if record['Label'] != "Transaction" and record['Label'] != "FirstPartyFraudster" and record['Label'] != "Mule"  and record['Label'] != "SecondPartyFraudSuspect" and record['Label'] != "SecondPartyFraud": 
                labels.append(record['Label'])
                count.append(record['Count'])
        plt.style.use('seaborn')
        plt.barh(labels, count)
        plt.title("Number of nodes for each label")
        #plt.show()

        x = []
        percent = []
        for record in results:
            if record['Label'] == "Merchant" or record['Label'] == "Client" or record['Label'] == "Bank": 
                x.append(record['Label'])
                percent.append(record['Count'])

        percent = np.array(percent)

        percent = percent / np.sum(percent) * 100
        

        patches, texts = plt.pie(percent, startangle=90, radius=1.2)
        labels = ['{0} - {1:1.2f} %'.format(i,j) for i,j in zip(x, percent)]

        sort_legend = True
        if sort_legend:
            patches, labels, dummy =  zip(*sorted(zip(patches, labels, percent),
                                                key=lambda x: x[2],
                                                reverse=True))

        plt.legend(patches, labels, loc='upper left', bbox_to_anchor=(-0.35, 1.), fontsize=8)
        plt.title("Types of agents distribution")
        #plt.show()

#count number of relationships
def count_relationships():
    cypher_query = '''
        CALL db.relationshipTypes() YIELD relationshipType as type
        CALL apoc.cypher.run('MATCH ()-[:`'+type+'`]->() RETURN count(*) as count', {})
        YIELD value
        RETURN type AS Relationship, value.count AS Count
    '''

    with driver.session(database="neo4j") as session:
        results = session.read_transaction(
            lambda tx: tx.run(cypher_query).data())

        for record in results:
            print(record)

#list all types of transactions, count them and make charts
#Note that transaction is the general type and include all the different types of transactions
def transactions():
    cypher_query = '''
        MATCH (t:Transaction)
        WITH count(t) AS globalCnt
        UNWIND ['CashIn', 'CashOut', 'Payment', 'Debit', 'Transfer'] AS txType
        CALL apoc.cypher.run('MATCH (t:' + txType + ')
            RETURN count(t) AS txCnt', {})
        YIELD value
        RETURN txType, value.txCnt AS NumberOfTransactions,
        round(toFloat(value.txCnt)/toFloat(globalCnt), 2) AS `%Transactions`
        ORDER BY `%Transactions` DESC;

    '''

    with driver.session(database="neo4j") as session:
        results = session.read_transaction(
            lambda tx: tx.run(cypher_query).data())

        percent = []
        x = []
        for i in range(len(results)):
            percent.append(results[i]['%Transactions'] * 100)
            x.append(results[i]['txType'])

        patches, texts = plt.pie(percent, startangle=90, radius=1.2)
        labels = ['{0} - {1:1.2f} %'.format(i,j) for i,j in zip(x, percent)]

        sort_legend = True
        if sort_legend:
            patches, labels, dummy =  zip(*sorted(zip(patches, labels, percent),
                                                key=lambda x: x[2],
                                                reverse=True))

        plt.legend(patches, labels, loc='upper left', bbox_to_anchor=(-0.35, 1.), fontsize=8)
        plt.title("Types of transactions distribution")
        plt.show()

#the first query counts the number of shared identifiers for each pair of clients
#the second query is the number of unique clients sharing identifiers
#There are three types of personally identifiable information (PII) in this dataset - SSN, Email and Phone Number
def clients_sharing_PII():
    cypher_query = '''
        MATCH (c1:Client)-[:HAS_EMAIL|:HAS_PHONE|:HAS_SSN]->(n) <-[:HAS_EMAIL|:HAS_PHONE|:HAS_SSN]-(c2:Client)
        WHERE id(c1) < id(c2)
        RETURN c1.id, c2.id, count(*) AS freq
        ORDER BY freq DESC;
    '''

    with driver.session(database="neo4j") as session:
        results = session.read_transaction(
            lambda tx: tx.run(cypher_query).data())

        for record in results:
            print(record)
    count_1=0 #number of users sharing 1 PII
    count_2=0 #number of users sharing 2 PII
    count_3=0 #number of users sharing 3 PII
    for record in results:
        if record['freq']==1 :
            count_1+=1
        elif record['freq']==2 :
            count_2+=1
        else:
            count_3+=1
    plt.style.use('seaborn')
    plt.bar(["sharing 1 identifier", "sharing 2 indentifiers", "sharing 3 identifiers"], [count_1, count_2, count_3])
    plt.title("Number of shared identifiers for each pair of clients")
    plt.show()

    cypher_query = '''
        MATCH (c1:Client)-[:HAS_EMAIL|:HAS_PHONE|:HAS_SSN]->(n) <-[:HAS_EMAIL|:HAS_PHONE|:HAS_SSN]-(c2:Client)
        RETURN count(DISTINCT c1.id) AS freq;
    '''

    with driver.session(database="neo4j") as session:
        results = session.read_transaction(
            lambda tx: tx.run(cypher_query,
                            name="MYrsa").data())

#Create a new relationship to connect clients that share identifiers and add the number of shared identifiers as a property on that relationship
def create_new_relationship():
    cypher_query = '''
        MATCH (c1:Client)-[:HAS_EMAIL|:HAS_PHONE|:HAS_SSN] ->(n)<- [:HAS_EMAIL|:HAS_PHONE|:HAS_SSN]-(c2:Client)
        WHERE id(c1) < id(c2)
        WITH c1, c2, count(*) as cnt
        MERGE (c1) - [:SHARED_IDENTIFIERS {count: cnt}] -> (c2);
    '''

    with driver.session(database="neo4j") as session:
        results = session.write_transaction(
            lambda tx: tx.run(cypher_query,
                            name="MYrsa").data())

#project the graph with clients and shared_identifiers and store it in memory
def project_graph():
    cypher_query = '''
        CALL gds.graph.project('wcc',
            {
                Client: {
                    label: 'Client'
                }
            },
            {
                SHARED_IDENTIFIERS:{
                    type: 'SHARED_IDENTIFIERS',
                    orientation: 'UNDIRECTED',
                    properties: {
                        count: {
                            property: 'count'
                        }
                    }
                }
            }
        ) YIELD graphName,nodeCount,relationshipCount,projectMillis;
    '''
    
    with driver.session(database="neo4j") as session:
        results = session.write_transaction(
            lambda tx: tx.run(cypher_query).data())

#run weakly connected components on the projected graph to find clusters of clients sharing PII and write results in the graph
def write_wcc():
    cypher_query = '''
        CALL gds.wcc.stream('wcc',
            {
                nodeLabels: ['Client'],
                relationshipTypes: ['SHARED_IDENTIFIERS'],
                consecutiveIds: true
            }
        )
        YIELD componentId, nodeId
        WITH componentId AS cluster, gds.util.asNode(nodeId) AS client
        WITH cluster, collect(client.id) AS clients
        WITH cluster, clients, size(clients) AS clusterSize WHERE clusterSize > 1
        UNWIND clients AS client
        MATCH (c:Client) WHERE c.id = client
        SET c.firstPartyFraudGroup=cluster;
    '''

    with driver.session(database="neo4j") as session:
        results = session.write_transaction(
            lambda tx: tx.run(cypher_query).data())

#Compute pairwise similarity scores using Jaccard metric and write as relationships
def similarity_scores():
    #in order to use similarity algorithm, we first have to create a graph projection 
    #so we project client nodes and three identifiers nodes into memory.
    cypher_query = '''
        MATCH(c:Client) WHERE c.firstPartyFraudGroup is not NULL
        WITH collect(c) as clients
        MATCH(n) WHERE n:Email OR n:Phone OR n:SSN
        WITH clients, collect(n) as identifiers
        WITH clients + identifiers as nodes

        MATCH(c:Client) -[:HAS_EMAIL|:HAS_PHONE|:HAS_SSN]->(id)
        WHERE c.firstPartyFraudGroup is not NULL
        WITH nodes, collect({source: c, target: id}) as relationships

        CALL gds.graph.project.cypher('similarity',
            "UNWIND $nodes as n RETURN id(n) AS id,labels(n) AS labels",
            "UNWIND $relationships as r RETURN id(r['source']) AS source, id(r['target']) AS target, 'HAS_IDENTIFIER' as type",
            { parameters: {nodes: nodes, relationships: relationships}}
        )
        YIELD graphName, nodeCount, relationshipCount, projectMillis
        RETURN graphName, nodeCount, relationshipCount, projectMillis
    '''
    
    with driver.session(database="neo4j") as session:
        results = session.write_transaction(
            lambda tx: tx.run(cypher_query,
                            name="MYrsa").data())
    
    #Write similarity scores to in-memory graph (mutate in-memory graph)
    cypher_query = '''
        CALL gds.nodeSimilarity.mutate('similarity',
            {
                topK:15,
                mutateProperty: 'jaccardScore',
                mutateRelationshipType:'SIMILAR_TO'
            }
        );
    '''

    with driver.session(database="neo4j") as session:
        results = session.write_transaction(
            lambda tx: tx.run(cypher_query,
                            name="MYrsa").data())

    #we write back the property from in-memory graph to the database and use it for further analysis
    cypher_query = '''
        CALL gds.graph.writeRelationship('similarity', 'SIMILAR_TO', 'jaccardScore');
    '''

    with driver.session(database="neo4j") as session:
        results = session.write_transaction(
            lambda tx: tx.run(cypher_query,
                            name="MYrsa").data())

#compute the sum of similar_to and store it in the nodes by using weighted (by jaccard score) degree centrality algorithm
def find_FirstPartyFraudster():
    #apply degree centrality (imported from gds) and write back centrality scores as firstPartyFraudScore to the database
    cypher_query = '''
    CALL gds.degree.write('similarity',
        {
            nodeLabels: ['Client'],
            relationshipTypes: ['SIMILAR_TO'],
            relationshipWeightProperty: 'jaccardScore',
            writeProperty: 'firstPartyFraudScore'
        }
    );
    '''
    
    with driver.session(database="neo4j") as session:
        results = session.write_transaction(
            lambda tx: tx.run(cypher_query,
                            name="MYrsa").data())
    #We find clients with first-party fraud score greater than 0.95 and label those top 95th percentile clients as fraudsters.
    cypher_query = '''
        MATCH(c:Client)
        WHERE c.firstPartyFraudScore IS NOT NULL
        WITH percentileCont(c.firstPartyFraudScore, 0.95) AS firstPartyFraudThreshold

        MATCH(c:Client)
        WHERE c.firstPartyFraudScore > firstPartyFraudThreshold
        SET c:FirstPartyFraudster;
    '''

    with driver.session(database="neo4j") as session:
        results = session.write_transaction(
            lambda tx: tx.run(cypher_query,
                            name="MYrsa").data())

#START Second-Party Fraudsters analysis
#create transfer_to relationships in both directions to link firstPartyFraud with possible secondPartyFraud
def create_transfer_to():
    cypher_query = '''
        MATCH (c1:FirstPartyFraudster)-[]->(t:Transaction)-[]->(c2:Client)
        WHERE NOT c2:FirstPartyFraudster
        WITH c1, c2, sum(t.amount) AS totalAmount
        SET c2:SecondPartyFraudSuspect
        CREATE (c1)-[:TRANSFER_TO {amount:totalAmount}]->(c2);
    '''
    
    with driver.session(database="neo4j") as session:
        results = session.write_transaction(
            lambda tx: tx.run(cypher_query,
                            name="MYrsa").data())
    
    cypher_query = '''
        MATCH (c1:FirstPartyFraudster)<-[]-(t:Transaction)<-[]-(c2:Client)
        WHERE NOT c2:FirstPartyFraudster
        WITH c1, c2, sum(t.amount) AS totalAmount
        SET c2:SecondPartyFraudSuspect
        CREATE (c1)<-[:TRANSFER_TO {amount:totalAmount}]-(c2);
    '''

    with driver.session(database="neo4j") as session:
        results = session.write_transaction(
            lambda tx: tx.run(cypher_query,
                            name="MYrsa").data())

#run weakly connected components to identify networks of clients who are connected to first party fraudsters
def write_wcc_secondPartyFraud():
    cypher_query = '''
        CALL gds.graph.project('SecondPartyFraudNetwork',
            'Client',
            'TRANSFER_TO',
            {relationshipProperties:'amount'}
        );
    '''
    
    with driver.session(database="neo4j") as session:
        results = session.write_transaction(
            lambda tx: tx.run(cypher_query,
                            name="MYrsa").data())
    
    cypher_query = '''
        CALL gds.wcc.stream('SecondPartyFraudNetwork')
        YIELD nodeId, componentId
        WITH gds.util.asNode(nodeId) AS client, componentId AS clusterId
        WITH clusterId, collect(client.id) AS cluster
        WITH clusterId, size(cluster) AS clusterSize, cluster
        WHERE clusterSize > 1
        UNWIND cluster AS client
        MATCH(c:Client {id:client})
        SET c.secondPartyFraudGroup=clusterId;
    '''

    with driver.session(database="neo4j") as session:
        results = session.write_transaction(
            lambda tx: tx.run(cypher_query,
                            name="MYrsa").data())

#use pagerank to find out who among the suspects have relatively higher fraud scores
def pageRank_secondPartyFraud():
    cypher_query = '''
        CALL gds.pageRank.stream('SecondPartyFraudNetwork',
            {relationshipWeightProperty:'amount'}
        )YIELD nodeId, score
        WITH gds.util.asNode(nodeId) AS client, score AS pageRankScore

        WHERE client.secondPartyFraudGroup IS NOT NULL
                AND pageRankScore > 0 AND NOT client:FirstPartyFraudster

        MATCH(c:Client {id:client.id})
        SET c:SecondPartyFraud
        SET c.secondPartyFraudScore = pageRankScore;
    '''
    
    with driver.session(database="neo4j") as session:
        results = session.write_transaction(
            lambda tx: tx.run(cypher_query,
                            name="MYrsa").data())

#run the above functions needed to identify and label first party fraudsters and second party fraudsters
def runAll():
    create_new_relationship()
    project_graph()
    write_wcc()
    similarity_scores()
    find_FirstPartyFraudster()
    create_transfer_to()
    write_wcc_secondPartyFraud()
    pageRank_secondPartyFraud()


#close connection to local neo4j database
driver.close()