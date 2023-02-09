[![pipeline status](https://gitlab.cs.uni-saarland.de/bigdata/mutable/mutable/badges/main/pipeline.svg)](https://gitlab.cs.uni-saarland.de/bigdata/mutable/mutable/-/commits/main)
[![coverage report](https://gitlab.cs.uni-saarland.de/bigdata/mutable/mutable/badges/main/coverage.svg)](http://deeprig02.cs.uni-saarland.de/mutable/coverage/)
[![Discord](https://img.shields.io/discord/692292755422052372?label=Discord&logo=Discord&style=flat)](https://discord.gg/JHwTZ24)

# mu*t*able
**A Database System for Research**

© Saarland University, 2022

## What is mu*t*able?

mu*t*able is a relational main-memory database system.
With mu*t*able, our goal is to develop a database system that allows for efficient prototyping of research ideas while
striving for performance that is competitive to state-of-the-art systems.

#### Defining Papers

- mu*t*able: A Modern DBMS for Research and Fast Prototyping @[CIDR 2023](https://www.cidrdb.org/cidr2023/index.html) [[PDF]](https://bigdata.uni-saarland.de/publications/Haffner,%20Dittrich%20-%20mutable:%20A%20Modern%20DBMS%20for%20Research%20and%20Fast%20Prototyping%20@CIDR2023.pdf)
  <details><summary>Abstract</summary><blockquote>
  Few to zero DBMSs provide extensibility together with implementations of modern concepts, like query compilation for example. We see this as an impeding factor in academic research in our domain. Therefore, in this work, we present mutable, a system developed at our group, that is fitted to academic research and education. mutable features a modular design, where individual components can be composed to form a complete system. Each component can be replaced by an alternative implementation, thereby mutating the system. Our fine-granular design of components allows for precise mutation of the system. Metaprogramming and just-in-time compilation are used to remedy abstraction overheads. In this demo, we present the high-level design goals of mutable, discuss our vision of a modular design, present some of the components, provide an outlook to research we conducted within mutable, and demonstrate some developer-facing features.
  </blockquote>
</details>

- A Simplified Architecture for Fast, Adaptive Compilation and Execution of SQL Queries @[EDBT 2023](http://edbticdt2023.cs.uoi.gr/?contents=accepted-papers-research-track.html) [[PDF]](https://bigdata.uni-saarland.de/publications/Haffner,%20Dittrich%20-%20A%20Simplified%20Architecture%20for%20Fast,%20Adaptive%20Compilation%20and%20Execution%20of%20SQL%20Queries%20@EDBT2023.pdf)
  <details><summary>Abstract</summary><blockquote>
  Query compilation is crucial to efficiently execute query plans. In the past decade, we have witnessed considerable progress in this field, including compilation with LLVM, adaptively switching from interpretation to compiled code, as well as adaptively switching from non-optimized to optimized code. All of these ideas aim to reduce latency and/or increase throughput. However, these approaches require immense engineering effort, a considerable part of which includes reengineering very fundamental techniques from the compiler construction community, like register allocation or machine code generation - techniques studied in this field for decades.
  In this paper, we argue that we should design compiling query engines conceptually very differently: rather than racing against the compiler construction community - a race we cannot win in the long run - we argue that code compilation and execution techniques should be fully delegated to an existing engine rather than being reinvented by database architects. By carefully choosing a suitable code compilation and execution engine we are able to get just-in-time code compilation (including the full range from non-optimized to fully optimized code) as well as adaptive execution in the sense of dynamically replacing code at runtime - for free! Moreover, as we rely on the vibrant compiler construction community, it is foreseeable that we will easily benefit from future improvements without any additional engineering effort. We propose this conceptual architecture using WebAssembly and V8 as an example. In addition, we implement this architecture as part of a real database system: mutable. We provide an extensive experimental study using TPC-H data and queries. Our results show that we are able to match or even outperform state-of-the-art systems like HyPer.
  </blockquote>
</details>

- Efficiently Computing Join Orders with Heuristic Search @[SIGMOD 2023](https://2023.sigmod.org/info-coming-soon.shtml) [[PDF]](https://bigdata.uni-saarland.de/publications/Haffner,%20Dittrich%20-%20Efficiently%20Computing%20Join%20Orders%20with%20Heuristic%20Search%20@SIGMOD2023.pdf)
  <details><summary>Abstract</summary><blockquote>
  Join order optimization is one of the most fundamental problems in processing queries on relational data. It has been studied extensively for almost four decades now. Still, because of its NP hardness, no generally efficient solution exists and the problem remains an important topic of research. The scope of algorithms to compute join orders ranges from exhaustive enumeration, to combinatorics based on graph properties, to greedy search, to genetic algorithms, to recently investigated machine learning. A few works exist that use heuristic search to compute join orders. However, a theoretical argument why and how heuristic search is applicable to join order optimization is lacking. In this work, we investigate join order optimization via heuristic search. In particular, we provide a strong theoretical framework, in which we reduce join order optimization to the shortest path problem. We then thoroughly analyze the properties of this problem and the applicability of heuristic search. We devise crucial optimizations to make heuristic search tractable. We implement join ordering via heuristic search in a real DBMS and conduct an extensive empirical study. Our findings show that for star- and clique-shaped queries, heuristic search finds optimal plans an order of magnitude faster than current state of the art. Our suboptimal solutions further extend the cost/time Pareto frontier.
  </blockquote>
</details>

## Contributors

See [`CONTRIBUTORS.md`](CONTRIBUTORS.md).

## License

Our software "mutable" is licensed under [AGPL v3](https://www.gnu.org/licenses/agpl-3.0.en.html).
A [copy of the license file](LICENSE) can be found within this project.
If not explicitly stated otherwise, this license applies to all files in this project.

## Getting started

- Make sure you have all the required [preliminaries](doc/preliminaries.md).
- Read how to [set up mu*t*able](doc/setup.md).
- Please read our [contribution guideline](doc/contribution-guideline.md).

## Important Links

- [Wiki](https://gitlab.cs.uni-saarland.de/bigdata/mutable/mutable/-/wikis/home)
- [Benchmarks](https://cb.mutable.uni-saarland.de/)
- [Documentation](http://deeprig02.cs.uni-saarland.de/mutable/doxy/) (only reachable from within our subnet)
- [Coverage](http://deeprig02.cs.uni-saarland.de/mutable/coverage/) (only reachable from within our subnet)
