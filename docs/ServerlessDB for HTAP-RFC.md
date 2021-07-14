# ServerlessDB for HTAP

- Author(s):Gang Xue, Zheng Shen, Pixian Shi, Jia Zou, Haoqi Chen

- Discussion PR: 

- Tracking Issue: 

  

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)


  ### Introduction

  ​      This document mainly introduces how to provide serverless db services based on TIDB on the cloud, focusing on how to dynamically scale up and down the compute storage nodes based on business load changes to achieve zero user perception. To ensure that the database service process, always maintain the best match between business load and background resources, thus helping users to maximize cost savings.

  ### Motivation or Background

  ​    While TIDB offers cloud services, there are a number of issues.

  1. When users order, they need to select the compute node and storage node specifications, and it is difficult for them to choose the right specifications, either by choosing smaller or larger ones, or the business load simply cannot be evaluated, resulting in users never being able to choose the right specifications.
  2. After the business load rises, you need to manually determine when to expand capacity, what resources to expand, and how much to expand. In practice, it is difficult to respond to the scenario of extremely rapid load changes in a timely manner, thus causing business performance fluctuations.
  3. After the business load drops, you need to manually judge when to shrink the capacity, what resources to shrink, and how much to shrink. If the user makes a wrong judgment, it will cause business performance fluctuations.
  4. If the business load changes very frequently, the manual implementation of expansion and shrinkage work is very burdensome. If the system is not expanded, the business performance will be degraded, and if the system is not scaled down, the resources will be wasted.
  5. It is difficult to achieve zero user awareness when scaling up or down. In case of connection pooling or long connections, it is even more impossible to do both of the following：
     - When scaling, if the client is using connection pools or long connections, it is not possible to break up the load to the additional compute nodes.
     - When scaling down, if the client is using a connection pool or a long connection, there is no guarantee of zero user awareness because you kill the compute node and if there is a connection on it, the client reports an exception.

  ### Detailed Design

  ​	In order to implement tidb serverless, we designed the proxy module and serverless module. The proxy module does permission control, computation under low load, and traffic forwarding under high load, while the serverless module mainly manages tidb-server instances and smoothly scales tidb-server.

  ![architecture](https://github.com/tidb-incubator/Serverlessdb-for-HTAP/blob/main/docs/architecture.png)

  #### proxy module

  - Low-load computing

    The proxy module acts as a tidb-server under low load, interacting directly with pd and tikv and returning sql execution results to the client.

  - High-load traffic forwarding

    Under high load, the proxy module mainly implements sql processing and traffic forwarding functions. sql processing mainly includes sql parsing, sql optimization, sql execution plan generation, and estimation of sql execution cost, while traffic forwarding mainly carries out load balancing according to the cost of sql, and forwards the sql execution plan to the regular tidb-server instance for execution.

  - Medium-load

    The proxy can be used as both a compute node and a proxy node. For example, for some point-check SQL, the proxy directly completes the calculation without forwarding to other tidb nodes. Some relatively complex SQL is forwarded to other nodes.

  - The proxy will establish connections to other TIDB nodes, each corresponding to a pool of connections. After the user request comes in, a series of calculations are performed, and finally, based on the cost, a suitable TIDB node is selected, and then a connection is selected from the pool of connections corresponding to this TIDB node for the specific task.

  ​       From the above description, there will be three roles for the proxy: pure compute node role, pure proxy role, and mixed compute and proxy role. These three roles will be dynamically adjusted online based on the business load.

  #### serverless module

  - tidb-server instance management

    ​     Control the specification of compute and storage instances and the number of instances, support load-based and rule-based elastic scaling, support compute/storage node scale up/down, scale out/in. 

  - Smooth expansion and contraction tidb-server

    ​     When expanding, the serverless module registers new instances with the proxy module, and when scaling down, the serverless module deletes instances with the proxy module. After each operation, the proxy module will dynamically load balance to existing instances to achieve smooth business migration.

  ### Test Design

  #### Functional Tests

  - Test proxy three role conversion logic, pure compute role ->mixed role (part compute, part proxy) ->pure proxy role
  - Test whether the business load model is reasonable, such as when to expand the capacity and how much. When to scale down and how many instances.
  - How to distinguish between TP and AP business SQL.
  - Connection pool management tests, how many connection pools correspond to different sizes of compute nodes, and if connection pools are allocated and reclaimed.
  - Business load testing, when receiving SQL requests, how to choose the right compute node based on the business load model.

  #### Scenario Tests

  - Test whether the corresponding computing and storage resources are reasonable under different business load pressure, and whether the business performance indicators are always kept in a reasonable range.
  - Test the sudden increase in business load, test how long it takes to complete the expansion, and whether the load is dispersed to all nodes after the expansion.
  - Test how long it takes to complete the shrinkage after the business load drops, and test whether the shrinkage is zero-aware to the business.
  - Test whether the performance loss caused by the proxy is within a reasonable range.

  #### Compatibility Tests

  - N/A

  #### Benchmark Tests

  - Keep SQL processing latency within reasonable limits during dynamic load changes.

  ### Impacts & Risks

  Advantages of Serverless DB database services, which can dynamically allocate resources based on business load changes. In general, ensuring that the system allocates just enough resources to meet business needs at any given time. Thus, under the premise of ensuring business performance, the cost of database service can be saved to the maximum extent. The whole process has zero user awareness.

  Severless DB database service itself also has some risk points,

  1. It is difficult to deal with the scene of drastic changes in business load, such as the TPS rising from 100 to 100 W in a very short time, which will cause business performance fluctuations, and some SQL Delay reaches 5s, and lasts for about 30s. 
  2. It is difficult to establish the most correct business load model, and there is a risk of load imbalance among some computing nodes. 
  3. Due to the introduction of proxy, there will be performance loss. Ideally, the performance loss is expected to be controlled within 15%

  ### Investigation & Alternatives

  The existing products with serverless capability are Tencent's tdsql-c and aurora products, but they are not open source, while the architecture of the two products is quite different from tidb, so we did not learn from them.

  ### Unresolved Questions

  Nothings

  
