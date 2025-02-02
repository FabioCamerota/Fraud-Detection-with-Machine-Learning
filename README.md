# Fraud detection with Machine Learning and Neo4j

Electronic money transactions generate large amount of data. <br /> Banks all over the world try to analyse this data to improve their service and provide a better experience to their customers.<br /> 
This network of transactions can be represented as a graph, which can be easily stored on a graph database such as Neo4j. <br /> 
One of the most interesting analysis which can be performed on the graph is called fraudsters detection, where it's possible to identify customers who operate for the purpose of scamming people.<br /> 
The goal of this project is to explore how Neo4j can be used to perform such type of analysis. In particular, we replicated the analysis performed by the Neo4j team on the PaySim dataset and we studied how working only on a subset of the graph affects the results.
This last experiment is extremely useful to understand how much data is needed to get reliable results and if it's easy to handle situations where computational resources are not enough to analyse the whole graph.<br /> 
The dataset used is the PaySim dataset that contains data generated by a financial simulator that generates fake mobile money transactions based on an original dataset. <br /> 
Most of the code we wrote is based on Neo4j GDS library, which provides a collection of graph analysis algorithms.
<br /> 

**For further details see "Report.pdf"**.

**Authors**:

- Fabio Camerota
- Matteo Santoro
- Giovanni Castellano
- Barth Niklas
