# The SQL Decomposer Manifesto

Inefficiencies, easily introduced, are also easily looked over. 'As long as it works' is a good enough maxim to follow in development and in life until a dead end is hit. When this happens people will generally adopt the knee-jerk viewpoint that they have exhausted all options and that the task at hand is impossible due to limitations of some kind. This document posits that in the realm of Database querying such as in general life, compartmentalization of a problem into smaller independent milestones.

Focusing on SQL, the method to implement this approach stems from a very simple belief:

>If a given query is made entirely of CTEs and each CTE of an SQL script is able to independantly yield a materialized table, there should be *no scenario* where the overall query times out. 

In these situations (timeouts and the like), the root problem can be found in a dichotomy very prevalent in the wider area of Computer Science: the trade-offs between space and time complexity. When a timeout happens time complexity needs to be lowered, even if at the cost of space complexity.

Enter the **Decomposer**, an application that aims to do exactly that. Very directly it is a program that through simple lexical analysis is able to filter and 'decompose' a given query into independant components saved locally before using the serialized data to 'recompile' for the desired result in memory in a faster, cleaner, clearly orchestrated manner.