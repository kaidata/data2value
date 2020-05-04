- tightly-integrated components of feedback loops involving dynamic, real-time decision making.
- computation with millisecond latency at high throughput, adaptive construction of arbitrary task graphs, and execution of heterogeneous kernels over diverse sets of resources.

- **Performance Requirements**. Emerging ML applications have stringent latency and throughput requirements.
  - R1: Low latency. The real-time, reactive, and interactive nature of emerging ML applications calls for
    fine-granularity task execution with millisecond end to end latency [8].
  - R2: High throughput. The volume of microsimulations required both for training [16] as well as for inference during deployment [19] necessitates support for high-throughput task execution on the order of millions of tasks per second.
- **Execution Model Requirements**. Though many existing parallel execution systems [9, 21] have gotten great
  mileage out of identifying and optimizing for common computational patterns, emerging ML applications require far greater flexibility [10].
  - R3: Dynamic task creation. RL primitives such as Monte Carlo tree search may generate new tasks during
    execution based on the results or the durations of other tasks. 
  - R4: Heterogeneous tasks. Deep learning primitives and RL simulations produce tasks with widely different
    execution times and resource requirements. Explicit system support for heterogeneity of tasks and resources is essential for RL applications.
  - R5: Arbitrary dataflow dependencies. Similarly, deep learning primitives and RL simulations produce arbitrary and often fine-grained task dependencies (not restricted to bulk synchronous parallel).
- **Practical Requirements.**
  - R6: Transparent fault tolerance. Fault tolerance remains a key requirement for many deployment scenarios, and supporting it alongside high-throughput and non-deterministic tasks poses a challenge.
  - R7: Debuggability and Profiling. Debugging and performance profiling are the most time-consuming aspects of writing any distributed application. This is especially true for ML and RL applications, which are often compute-intensive and stochastic.