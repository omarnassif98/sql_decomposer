# The SQL Decomposer Manifesto

Inefficiencies, easily introduced, are also easily overlooked. “As long as it works” is a good enough maxim to follow in development, as well as in wider life, until a dead end is hit. When this happens, people will generally adopt the knee-jerk viewpoint that they have exhausted all options and that the task at hand is impossible due to limitations of some kind. This document posits that in the realm of database querying, as in general life, compartmentalization of an issue into smaller independent milestones will open the door to achieving the desired outcome by way of increment.

Focusing on SQL, the method to implement this approach stems from a very simple belief:

> If a given query is made entirely of CTEs and each CTE of an SQL script is able to independently yield a materialized table, there should be *no scenario* where the overall query times out.

In these situations (timeouts and the like), the root problem can be found in a dichotomy very prevalent in the wider area of computer science: the trade-offs between space and time complexity. When a timeout happens, time complexity needs to be lowered, even if at the cost of space complexity.

Enter the **Decomposer**, an application that aims to do exactly that. Very directly, it is a program that, through simple lexical analysis, is able to filter and *decompose* a given query into independent components saved locally before using the serialized data to *recompose* the desired result in memory in a faster, cleaner, and more clearly orchestrated manner.