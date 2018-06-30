# GraphX - Outgoing Edges
By Shuning Zhao May 2018

Given a directed graph, compute the average length of the out-going edges for each node, and sort the nodes according to the average lengths in ascending order.

**Input files:**

In the input file, each line is in format of:  
“EdgeId FromNodeId ToNodeId Distance”.

![](https://github.com/ShuningZhao/GraphX-OutgoingEdges/blob/master/Images/01Graph.png?raw=true)

In the above example, the input is like:

![](https://github.com/ShuningZhao/GraphX-OutgoingEdges/blob/master/Images/02Input.png?raw=true)

This sample file “tiny-graph.txt” can be found [here](https://github.com/ShuningZhao/GraphX-OutgoingEdges/blob/master/tiny-graph.txt).

**Output:**  
Each line in the single output file is in format of “NodeID\tAverage length
of out-going edges”. The average length is of double precision. Remove the
nodes that have no out-going edges. Given the example graph, the output
file is like:

![](https://github.com/ShuningZhao/GraphX-OutgoingEdges/blob/master/Images/03Output.png?raw=true)

**Files**  
The file [01NoGraphX](ps://github.com/ShuningZhao/GraphX-OutgoingEdges/blob/master/01NoGraphX.scala) contains the solution without using GraphX and the file [02GraphX](ps://github.com/ShuningZhao/GraphX-OutgoingEdges/blob/master/02GraphX.scala) contains the solution with GraphX.