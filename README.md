# TaskLib
A high-performance multi-threaded event-driven IO Library

(c) 2019 - Stew Forster <stew675@gmail.com>

TaskLib offers the following features
- An easy to use highly-performing event driven multi-threaded asynchronous IO Library API
- A fully event driven multi-threaded-IO MP-safe framework
- A full set of socket connect+IO calls with automated timeout handling
- Fully distributed connection acceptance for better acceptance serving
- A light-weight but fast and powerful timer queue mechanism built atop my paired heap
  library here: git@github.com:stew675/Paired-Heap-Library.git
- Ability to create multiple worker threads to distribute load across multiple CPUs or
  to simply let the library auto-detect the number to use
- Automated detection of the number of CPUs in the system, and binding the affinity of
  worker threads to specific CPUs based upon operational patterns seen
- Automated load balancing of tasks across CPUs and worker threads
- Adaptive task migration.  If one task is continually operating on another task (such
  as a proxy might, with one task reading from a service, and another task responding
  to a client), then both tasks will automatically migrate to the same worker thread
  for lower latency operations and better CPU cache coherency.
- Easily accessible micro-second resolution timing
- Instanced definitions allows for the creation of multiple services to use the
  library within the same process completely independently of each other
