- Ray implements a unified interface that can express both **task-parallel** and actor-based computations, supported by a single dynamic execution engine. 
- To meet the performance requirements, Ray employs a **distributed scheduler** and a **distributed and fault-tolerant store** to manage the system’s control state.
- Big Data led to the development of a plethora of frameworks for distributed data analysis, including **batch** [20, 64, 28], **streaming** [15, 39, 31], and **graph** [34,35, 24] processing systems.
- data-focused applications has expanded to encompass more complex artificial intelligence (AI) or machine learning (ML) techniques [30].
  - The paradigm case is that of supervised learning
- Emerging AI applications must increasingly operate in **dynamic environments**, react to changes in the environment, and take sequences of actions to accomplish long-term goals [8, 43]. They must aim not only to exploit the data gathered, but also to explore the space of possible actions. These broader requirements are naturally framed within the paradigm of reinforcement learning (RL). 
- RL deals with learning to operate continuously within an uncertain environment based on delayed and limited feedback [56].
- The central goal of an RL application is to learn a **policy**—a mapping from the state of the environment to a choice of action—that yields effective performance over time.
- Finding effective policies in large-scale applications requires three main capabilities. 
  - First, RL methods often rely on simulation[模拟] to evaluate policies. Simulations make it possible to explore many different choices of action sequences and to learn about the long-term consequences of those choices. 
  - Second, like their supervised learning counterparts, RL algorithms need to perform distributed training to improve the policy based on data generated through simulations or interactions with the physical environment. 
  - Third, policies are intended to provide solutions to control problems, and thus it is necessary to serve the policy in interactive closed-loop and open-loop control scenarios.
- These characteristics drive new systems requirements: a system for RL must **support fine-grained computations** (e.g., rendering actions in milliseconds when interacting with the real world, and performing vast numbers of simulations), must **support heterogeneity both in time** (e.g., a simulation may take milliseconds or hours) and in resource usage (e.g., GPUs for training and CPUs for simulations), and must support **dynamic execution**, as results of simulations or interactions with the environment can change future computations. Thus, we need a dynamic computation framework that handles millions of heterogeneous tasks per second at millisecond-level latencies.
- Simulation、Training、Serving
  - Bulk-synchronous parallel systems such as Map-Reduce [20], Apache Spark [64], and Dryad [28] do not
    support fine-grained simulation or policy serving. Task parallel systems such as CIEL [40] and Dask [48] provide
    little support for distributed training and serving. The same is true for streaming systems such as Naiad [39]
    and Storm [31]. Distributed deep-learning frameworks such as TensorFlow [7] and MXNet [18] do not naturally
    support simulation and serving. Finally, model-serving systems such as TensorFlow Serving [6] and Clipper [19]
    support neither training nor simulation.
- Ray, a general-purpose cluster-computing framework that enables simulation, training, and serving for RL applications.  The requirements of these workloads range from lightweight and stateless computations, such as for simulation, to long running and stateful computations, such as for training. To satisfy these requirements, Ray implements a unified
  interface that can express both task-parallel and actor based computations. Tasks enable Ray to efficiently and
  dynamically load balance simulations, process large inputs and state spaces (e.g., images, video), and recover
  from failures. In contrast, actors enable Ray to efficiently support stateful computations, such as model training, and
  expose shared mutable state to clients, (e.g., a parameter server). Ray implements the actor and the task abstractions
  on top of a single dynamic execution engine that is highly scalable and fault tolerant.
- To meet the performance requirements, Ray distributes two components that are typically centralized in existing
  frameworks [64, 28, 40]: (1) the task scheduler and (2) a metadata store which maintains the computation lineage
  and a directory for data objects. This allows Ray to schedule millions of tasks per second with millisecond-level
  latencies. Furthermore, Ray provides lineage-based fault tolerance for tasks and actors, and replication-based fault
  tolerance for the metadata store.
- We make the following contributions:
  - We design and build the first distributed framework that unifies training, simulation, and serving—
    necessary components of emerging RL applications. 
  - To support these workloads, we unify the actor and task-parallel abstractions on top of a dynamic task
    execution engine.
  - To achieve scalability and fault tolerance, we propose a system design principle in which control state
    is stored in a sharded metadata store and all other system components are stateless.
  - To achieve scalability, we propose a bottom-up distributed scheduling strategy.
- 

