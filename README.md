# IoT Data Pipeline implemented in Scala, Pekko

This project contains a containerized IoT data pipeline written in Scala using Pekko actors and streams.
It uses lazyly evaluated streams to produce sensor readings every 1/10th of a second and send them to mqtt via a sink.
The subscriber subscribes to the wildcard topic sensor, and for each different topic (sensor) in the wildcard, it produces a new actor that keeps the last state of the sensor that was written on the queue.

## Pekko executors: Fork-Join vs Virtual Threads

In the default implemention of the actor dispatcher (scheduler) the actors are dispatched on an executor called fork-join-executor (which is similar to an event loop). Each actor represents a task object that is pushed unto a deque of tasks that is executed on a platform thread.
This means that if the actor blocks waiting for example for IO, then the underlying platform thread is also blocked. An alternative executor more closely resembling Erlang's and Elixir's genserver can be implemented by using an executor based on virtual threads.

In Java 21, virtual threads where introduced. It is a lightweight thread that is managed by the runtime (similar to goroutines in golang), that can block and because it is managed by the runtime, it can be unpinned from the platform thread.
Pekko 1.2.0 (2025) with Java 24 now fully supports virtual-thread-executor. Actors can be deployed with simple blocking logic and the runtime will schedule them.

The current implementation of this project uses fork-join-executor (the default). Pekko also provides a thread-pool-executor that pushes each actor on it's own platform thread but this will be too expensive for using a thread for each sensor.
