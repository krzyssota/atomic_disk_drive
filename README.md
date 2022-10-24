# Distributed Systems Large Assignment 2
# Atomic Disk Drive

Your task is to implement a distributed block device which stores data in a distributed register. The solution shall take the form of a Rust library. A template, public tests and additional files are provided in this package.
Background

A distributed register consists of multiple processes (entities of the distributed register, not operating system processes) running in user space, possibly on multiple physical (or virtual) machines. A Linux block device driver communicates with the processes using TCP. The processes themselves communicate using TCP too. The processes can crash and recover at any time. A number of the processes is fixed before the system is run, and every process is given its own directory where it can store its internal files.

For full specifications refer to task.pdf file in the repository.
