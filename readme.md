# The SQL Decomposer Manifesto
The rationale behind this project centers around a very simple belief. 
>If a given query is made entirely of CTEs and each CTE of an SQL script is able to independantly yield a materialized table, there should be *no scenario* where the overall query times out. 

In these situations (timeouts and the like), the root problem can be found in a dichotomy very prevalent in the wider area of Computer Science: the trade-offs between space and time complexity. When a timeout happens time complexity needs to be lowered, even if at the cost of space complexity.

Enter the **Decomposer**, an application that aims to do exactly that. A program that through simple lexical analysis is able to filter and 'decompose' a given query into independant components saved locally before using the serialized data to 'recompile' for the desired result in memory.