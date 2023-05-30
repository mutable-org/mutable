[![pipeline status](https://gitlab.cs.uni-saarland.de/bigdata/mutable/mutable/badges/main/pipeline.svg)](https://gitlab.cs.uni-saarland.de/bigdata/mutable/mutable/-/commits/main)
[![coverage report](https://gitlab.cs.uni-saarland.de/bigdata/mutable/mutable/badges/main/coverage.svg)](http://deeprig02.cs.uni-saarland.de/mutable/coverage/)
[![Discord](https://img.shields.io/discord/692292755422052372?label=Discord&logo=Discord&style=flat)](https://discord.gg/JHwTZ24)

# mu*t*able
**A Database System for Research**

© Saarland University, 2023

## What is mu*t*able?

mu*t*able is a relational main-memory database system.
With mu*t*able, our goal is to develop a database system that allows for efficient prototyping of research ideas while
striving for performance that is competitive to state-of-the-art systems.

### Defining Papers

- mu*t*able: A Modern DBMS for Research and Fast Prototyping [@CIDR 2023](https://www.cidrdb.org/cidr2023/index.html) [[PDF]](https://bigdata.uni-saarland.de/publications/Haffner,%20Dittrich%20-%20mutable:%20A%20Modern%20DBMS%20for%20Research%20and%20Fast%20Prototyping%20@CIDR2023.pdf)
  <details><summary>Abstract</summary>
  <blockquote>
  Few to zero DBMSs provide extensibility together with implementations of modern concepts, like query compilation for example. We see this as an impeding factor in academic research in our domain. Therefore, in this work, we present mutable, a system developed at our group, that is fitted to academic research and education. mutable features a modular design, where individual components can be composed to form a complete system. Each component can be replaced by an alternative implementation, thereby mutating the system. Our fine-granular design of components allows for precise mutation of the system. Metaprogramming and just-in-time compilation are used to remedy abstraction overheads. In this demo, we present the high-level design goals of mutable, discuss our vision of a modular design, present some of the components, provide an outlook to research we conducted within mutable, and demonstrate some developer-facing features.
  </blockquote>
  <sup>
  <pre>
  @inproceedings{haffner23mutable,
      author    = {Haffner, Immanuel and Dittrich, Jens},
      title     = {mu\emph{t}able: A Modern DBMS for Research and Fast Prototyping},
      year      = {2023},
      publisher = {cidrdb.org}
  }
  </pre>
  </sup>
</details>

- A Simplified Architecture for Fast, Adaptive Compilation and Execution of SQL Queries [@EDBT 2023](http://edbticdt2023.cs.uoi.gr/?contents=accepted-papers-research-track.html) [[PDF]](https://bigdata.uni-saarland.de/publications/Haffner,%20Dittrich%20-%20A%20Simplified%20Architecture%20for%20Fast,%20Adaptive%20Compilation%20and%20Execution%20of%20SQL%20Queries%20@EDBT2023.pdf)
  <details><summary>Abstract</summary>
  <blockquote>
  Query compilation is crucial to efficiently execute query plans. In the past decade, we have witnessed considerable progress in this field, including compilation with LLVM, adaptively switching from interpretation to compiled code, as well as adaptively switching from non-optimized to optimized code. All of these ideas aim to reduce latency and/or increase throughput. However, these approaches require immense engineering effort, a considerable part of which includes reengineering very fundamental techniques from the compiler construction community, like register allocation or machine code generation - techniques studied in this field for decades.
  In this paper, we argue that we should design compiling query engines conceptually very differently: rather than racing against the compiler construction community - a race we cannot win in the long run - we argue that code compilation and execution techniques should be fully delegated to an existing engine rather than being reinvented by database architects. By carefully choosing a suitable code compilation and execution engine we are able to get just-in-time code compilation (including the full range from non-optimized to fully optimized code) as well as adaptive execution in the sense of dynamically replacing code at runtime - for free! Moreover, as we rely on the vibrant compiler construction community, it is foreseeable that we will easily benefit from future improvements without any additional engineering effort. We propose this conceptual architecture using WebAssembly and V8 as an example. In addition, we implement this architecture as part of a real database system: mutable. We provide an extensive experimental study using TPC-H data and queries. Our results show that we are able to match or even outperform state-of-the-art systems like HyPer.
  </blockquote>
  <sup>
  <pre>
  @inproceedings{haffner23simplified,
      author    = {Haffner, Immanuel and Dittrich, Jens},
      title     = {A Simplified Architecture for Fast, Adaptive Compilation and Execution of SQL Queries},
      year      = {2023},
      booktitle = {Proceedings of the 26th International Conference on Extending Database
                   Technology, {EDBT} 2023, Ioannina, Greece, March 28 - March 31, 2023},
      publisher = {OpenProceedings.org}
  }
  </pre>
  </sup>
</details>

- Efficiently Computing Join Orders with Heuristic Search [@SIGMOD 2023](https://2023.sigmod.org/info-coming-soon.shtml) [[PDF]](https://bigdata.uni-saarland.de/publications/Haffner,%20Dittrich%20-%20Efficiently%20Computing%20Join%20Orders%20with%20Heuristic%20Search%20@SIGMOD2023.pdf)
  <details><summary>Abstract</summary>
  <blockquote>
  Join order optimization is one of the most fundamental problems in processing queries on relational data. It has been studied extensively for almost four decades now. Still, because of its NP hardness, no generally efficient solution exists and the problem remains an important topic of research. The scope of algorithms to compute join orders ranges from exhaustive enumeration, to combinatorics based on graph properties, to greedy search, to genetic algorithms, to recently investigated machine learning. A few works exist that use heuristic search to compute join orders. However, a theoretical argument why and how heuristic search is applicable to join order optimization is lacking. In this work, we investigate join order optimization via heuristic search. In particular, we provide a strong theoretical framework, in which we reduce join order optimization to the shortest path problem. We then thoroughly analyze the properties of this problem and the applicability of heuristic search. We devise crucial optimizations to make heuristic search tractable. We implement join ordering via heuristic search in a real DBMS and conduct an extensive empirical study. Our findings show that for star- and clique-shaped queries, heuristic search finds optimal plans an order of magnitude faster than current state of the art. Our suboptimal solutions further extend the cost/time Pareto frontier.
  </blockquote>
  <sup>
  <pre>
  @inproceedings{haffner23joinorders,
      author    = {Haffner, Immanuel and Dittrich, Jens},
      title     = {Efficiently Computing Join Orders with Heuristic Search},
      year      = {2023},
      booktitle = {SIGMOD},
      publisher = {ACM}
  }
  </pre>
  </sup>
</details>


#### Podcast Episode about mu*t*able

Immanuel Haffner was guest on [Disseminate: The Computer Science Research Podcast](https://disseminatepodcast.podcastpage.io/).
Together with our host Jack Waudby, we discuss our CIDR 2023 paper and introduce the listener to mu*t*able.
[[LINK TO EPISODE]](https://disseminatepodcast.podcastpage.io/episode/immanuel-haffner-mutable-a-modern-dbms-for-research-and-fast-prototyping-21)



### Features

<details><summary><b>Arbitrary Data Layouts</b></summary>

A *domain-specific language* (DSL) to define *arbitrary* data layouts.
The DSL constructs an internal representation, that enables mu*t*able to understand the data layout and to generate optimized code to access the data.

#### Example

Assume you want to create a table with the following schema:

| <ins>id</ins> : INT(4) | name : CHAR(80) | salary : DOUBLE |
|-|-|-|

To create a **row-major** data layout for the table, in mu*t*able you can create a `DataLayout` object as shown below:

```cpp
DataLayout layout; // fresh, empty layout
auto &row = layout.add_inode( // create a row representation
    /* num_tuples= */ 1, // a row contains one tuple…
    /* stride_in_bits= */ 832 // see ①
);
row.add_leaf(
    /* type=   */ Type::Get_Integer(Type::TY_Vector, /* bytes= */ 4),
    /* idx=    */ 0, // 0 ↔ `id`
    /* offset= */ 0, // offset of `id` within a row
    /* stride= */ 0 // not applicate, element not repeated within row
);
row.add_leaf(
    /* type=   */ Type::Get_Char(Type::TY_Vector, /* characters= */ 80),
    /* idx=    */ 1, // 1 ↔ `name`
    /* offset= */ 32,
    /* stride= */ 0
);
row.add_leaf(
    /* type=   */ Type::Get_Double(Type::TY_Vector),
    /* idx=    */ 2, // 2 ↔ `salary`
    /* offset= */ 704, // 32 + 8 * 80 + 32 (pad to multiple of 64)
    /* stride= */ 0
);
row.add_leaf(
    /* type=   */ Type::Get_Bitmap(Type::TY_Vector, /* bits= */ 3), // one bit per attribute
    /* idx=    */ 3, // 3 ↔ "NULL bitmap"
    /* offset= */ 768, // 32 + 8 * 80 + 32 + 64
    /* stride= */ 0
);
```

<sup>
① The stride in bits for any `INode` must be specified at construction.
The stride is chosen, such that every row is suitably aligned to fulfill *self-alignment* of all its descendants.
Looking at the last leaf -- that for the NULL bitmap -- we see that the row must be at least 771 bits in size.
The leaf with the largest *alignment requirement* inside a row is `salary` of type `DOUBLE` with an alignment requirement of 64 bits.
Hence, we must ceil 771 to a whole multiple of 64 to accommodate sufficient space in a row *while* guaranteeing self-alignment of all leaves.
</sup>

<br>
<br>

Alternatively, we can create a *partition attributes accross* (**PAX**) layout for the table.
This is done in mu*t*able as shown below:

```cpp
DataLayout layout; // fresh, empty layout
auto &block = layout.add_inode(
    /* num_tuples= */ 704, // a PAX block contains 704 tuples…
    /* stride_in_bits= */ 512 * 1024 // and contains 512 KiB
);
block.add_leaf(
    /* type=   */ Type::Get_Integer(Type::TY_Vector, /* bytes= */ 4),
    /* idx=    */ 0, // 0 ↔ `id`
    /* offset= */ 0, // offset of `id` column within a PAX block
    /* stride= */ 32 // `id`s repreated with 32 bits stride
);
row.add_leaf(
    /* type=   */ Type::Get_Char(Type::TY_Vector, /* characters= */ 80),
    /* idx=    */ 1, // 1 ↔ `name`
    /* offset= */ 22'528, // 704 * 32
    /* stride= */ 640 // `name`s repeated with 640 bits stride
);
row.add_leaf(
    /* type=   */ Type::Get_Double(Type::TY_Vector),
    /* idx=    */ 2, // 2 ↔ `salary`
    /* offset= */ 473'088, // 704 * 32 + 704 * (8 * 80), already self-aligned
    /* stride= */ 64 // `salary`s repeated with 64 bits stride
);
row.add_leaf(
    /* type=   */ Type::Get_Bitmap(Type::TY_Vector, /* bits= */ 3), // one bit per attribute
    /* idx=    */ 3, // 3 ↔ "NULL bitmap"
    /* offset= */ 518'144, // 704 * 32 + 704 * (8 * 80) + 704 * 64
    /* stride= */ 8 // NULL bitmap repeated with stride of 8 bits ②
);
```

<sup>
② mu*t*able actually supports sub-byte strides, and we could in fact use 3 bits of stride for the NULL bitmap.
However, while sub-byte strides save memory by avoiding padding, they complicate the data acces logic.
In our example, we therefore opt for 8 bit stride to trade some unused bits for more efficient data accesses.
</sup>

<br>
<br>

It is also possible to nest `INode`s of the `DataLayout` to arbitrary depths.
This allows the creation of layouts such as *PAX-in-PAX* or *vertical partitioning*.

<br>
<br>

</details>

<details><summary><b>Cardinality Estimation with Sum-Product Networks</b></summary>

We implemented *relational sum-product networks* (RSPNs), as proposed in [DeepDB](http://www.vldb.org/pvldb/vol13/p992-hilprecht.pdf), in mu*t*able.
We achieve a relatively efficient implementation by implementing RSPN logic with [Eigen, a "C++ template library for linear algebra"](https://eigen.tuxfamily.org/index.php?title=Main_Page).

After loading data into a database, you can manually trigger training of RSPNs with our built-in command `\learn_spns`.
Documentation on the implementation of vanilla *sum-product networks* (SPNs) can be found [here](src/util/README.md) and the
database specific extension making them RSPNs can be found [here](src/catalog/README.md).

Disclaimer: Currently we have not yet implemented automatic updates of SPNs and string support. These are future tasks.


<br>
<br>

</details>

<details><summary><b>Injecting Cardinality Estimates</b></summary>

mu*t*able provides a method to inject cardinality estimates into the system, that will then be used for query optimization.
The cardinality estimates are specified in a JSON format, that is best described by an example.
Consider the following example query:

```sql
-- in database `demo`
SELECT COUNT(*)
FROM R, S, T
WHERE R.sid = S.id AND S.tid = T.id AND
      R.x > 42 AND S.y < 13;
```

There are up to six intermediate results that can occur during join ordering, namely {$R$}, {$S$}, {$T$}, {$R,S$}, {$S,T$}, and {$R,S,T$}, omitting Cartesian products.
We can specify the cardinality estimates that should be provided to the cardinality estimation component by providing the following JSON file to mu*t*able.

```json
{
    "demo": [
        { "relations": ["R"], "size": 9081},
        { "relations": ["S"], "size": 8108},
        { "relations": ["T"], "size": 361},
        { "relations": ["R", "S"], "size": 14447050},
        { "relations": ["S", "T"], "size": 7478},
        { "relations": ["R", "S", "T"], "size": 3374320}
    ]
}
```

When executing the above query in mu*t*able and providing the above cardinality JSON file, mu*t*able's query optimization will use these cardinalities to compute a plan.
For example, the output for the above query could be

```
ProjectionOperator {[ COUNT() :INT(8) ]}
` AggregationOperator [COUNT()] {[ COUNT() :INT(8) ]} <1>
  ` JoinOperator (R.fid_R1 = S.id) {[ R.id :INT(4), R.fid_R1 :INT(4), S.id :INT(4), S.fid_R2 :INT(4), T.id :INT(4) ]} <3.37432e+06>
    ` ScanOperator (R AS R) {[ R.id :INT(4), R.fid_R1 :INT(4) ]} <9081>
    ` JoinOperator (S.fid_R2 = T.id) {[ S.id :INT(4), S.fid_R2 :INT(4), T.id :INT(4) ]} <7478>
      ` ScanOperator (S AS S) {[ S.id :INT(4), S.fid_R2 :INT(4) ]} <8108>
      ` ScanOperator (T AS T) {[ T.id :INT(4) ]} <361>
```
 At the very end of each line, you find the cardinality estimates for the intermediate result produced by each operator, located in angular brackets `<,>`.

To provide such a cardinality file to mu*t*able, use the following command line arguments:

```sh
--cardinality-estimator Injected --use-cardinality-file "/path/to/file.json"
```

Intermediate results for which *no cardinality estimate* is specified will fall back to Cartesian product and print a warning to `stderr`.

<br>
<br>

</details>

<details><summary><b>Generating Fake Cardinalities</b></summary>

Generating fake cardinality estimates (sometimes called fake statistics) is useful to steer, test, or evaluate the query optimization process.

Therefore, we have built a tool `cardianlity_gen` that randomly generates cardinalities for all subsets of relations.
These cardinalities are output in JSON, in the format that is suitable for cardinality injection (see our **Injecting Cardinality Estimates** feature).

Our `cardinality_gen` takes as input a SQL file defining the database schema and a SQL query.
First, it constructs the query's query graph.
Along the graph, it then enumerates all subsets of joined relations using an efficient graph algorithm to enumerate all *connected subgraphs* (CSGs).
This is done in a bottom-up fashion, e.g. the cardinality of {$A$,$B$} is generated *after* the cardinalities for $A$ and $B$ have been generated.
Further, all cardinalities are generated to be *sane*, e.g. |$A\Join B$| must never be larger than $|A| * |B|$.

The randomized generation of cardinalities with `cardinality_gen` can be steered through command line arguments.
See `--help` for a complete list of parameters.
Important mentions are `--alpha` to steer the skew of join selectivities and `--uncorrelated` to decide whether to generate pairwise independent (uncorrelated) join selectivities.
Note, that pairwise independent join selectivities are practically impossible and with *too small* cardinalities, slight deviations from pairwise independent may manifest.

<br>
<br>

</details>

<details><summary><b>Just-in-Time Query Compilation</b></summary>

We initially created mu*t*able to enable our research on *just-in-time* (JIT) compilation of SQL queries to WebAssembly (Wasm).
By now, mu*t*able has a dedicated Wasm backend that performs very fast JIT compilation to Wasm and that delegates the generated Wasm code on to the embedded V8 engine for execution.
V8 takes care of JIT compiling Wasm to machine code, of applying compiler optimizations, and of adaptively switching from unoptimized to optimized code while the query is running.
See our EDBT 2023 paper for more information.

<br>
<br>

</details>

<details><summary><b>Code Generation DSL</b></summary>

To relief the programmer from tediously writing query compilation steps or Wasm code generation directly, we have built a *deeply-embedded domain-specific language* (deep DSL).
Our deep DSL is written in C++ and mimics C in syntax and semantics.
Our deep DSL can be mixed with regular C++ code to allow for understandable and maintainable code generation through meta programming.
This is best demonstrated by an example:

```cpp
auto gen_power(int exp)
{
    FUNCTION(power, int(int))
    {
        Var<int> res = 1;
        auto base = PARAMETER(0); // or `ThisFunction.parameter<0>()`

        while (exp > 0) {
            if (exp % 2 == 0) { // even exponent
                res = res * res; // b^(2n) = (b^n)²
                exp /= 2;
            } else { // odd exponent
                res *= base;
                exp -= 1;
            }
        }

        RETURN(res);
    }

    return power; // returns a handle to the generated `power` function
}

void demo()
{
    auto pow5 = gen_power(5);
    Var<int> a = pow5(2); // 2⁵
    Var<int> b = pow5(3); // 3⁵

    auto pow8 = gen_power(8);
    Var<int> c = pow8(2); // 2⁸
    Var<int> d = pow8(3); // 3⁸
}
```

Our DSL implements a type system that is less permissive than C/C++.
It prevents mixing signed and unsigned types, implicit casts that reduce precision, and implicit casts to/from floating-point representation.
Furthermore, our DSL supports *three-valued logic* (3VL): elements can be declared with template parameter `CanBeNull=true` to have ops perform 3VL.
This is a big relief when implementing database operator logic.
When attributes in a table are declared `NOT NULL`, then the code that would implement 3VL is actually never even generated.

<br>
<br>

</details>


<details><summary><b>Many Algorithms for Plan Enumeration</b></summary>

We have implemented a broad spectrum of algorithms for join ordering / plan enumeration.

Algorithms computing an optimal plan:

- $\textit{DP}_\textit{size}$
- $\textit{DP}_\textit{sub}$
- $\textit{DP}_\textit{ccp}$
- $\textit{TD}_\textit{basic}$
- $\textit{TD}_\textit{MinCutAGaT}$

Algorithms computing a potentially suboptimal plan:

- IK/KBZ (optimal on acyclic queries with pairwise independent join selectivities)
- *greedy operator ordering* (GOO)
- LinearizedDP: DP with search space linearization based on IK/KBZ

In addition, we have developed a novel algorithm that is based on a reduction of the join order optimization problem to heuristic search.
The plan enumeration algorithm is named `HeuristicSearch`.
See our SIGMOD 2023 paper for more information.

<br>
<br>

</details>

<details><summary><b>Visual Representation</b></summary>

mu*t*able can render some intermediate results during query processing as graphs in Graphviz, and also directly render them to PDF in case the necessary libraries are available on the system.
Flags `--astdot`, `--graphdot`, and `--plandot` render the respective query representation in Graphviz.
Then, if the Graphviz library is installed, the graph is directly rendered to PDF.
Otherwise, the *dot* representation of the graph is output.

<br>
<br>

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

- [Code Documentation](https://api.mutable.uni-saarland.de/) -- Doxygen documentation extracted from our in-source documentation.
- [Benchmarking Website](https://cb.mutable.uni-saarland.de/) -- Website visualizing our nightly benchmarks.
- [Coverage Reports](http://deeprig02.cs.uni-saarland.de/mutable/coverage/) -- LCOV coverage reports.
