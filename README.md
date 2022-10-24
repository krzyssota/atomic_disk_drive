# Distributed Systems Large Assignment 2
# Atomic Disk Drive

Your task is to implement a distributed block device which stores data in a distributed register. The solution shall take the form of a Rust library. A template, public tests and additional files are provided in this package.
Background

On UNIX-like operating systems, a block device is, simplifying, a device which serves random-access reads and writes of data in portions called blocks. Arguably, the most common types of block devices are disk drives (e.g., HDD and SSD). However, the term is also applied to various abstractions which implement the interface of the block device (and thus, from a user’s perspective, can be used like any other block device), but do not store data on physical devices. For instance, Atomic Disc Drive which stores data in a distributed register.

A distributed register consists of multiple processes (entities of the distributed register, not operating system processes) running in user space, possibly on multiple physical (or virtual) machines. A Linux block device driver communicates with the processes using TCP. The processes themselves communicate using TCP too. The processes can crash and recover at any time. A number of the processes is fixed before the system is run, and every process is given its own directory where it can store its internal files.

The smallest physical unit inside a block device is called a sector, and its size is specific to each device. A size of a block is always a multiple of the size of the sector. In the Atomic Disk Drive every sector is a separate atomic value called register (and thus it is said that the system supports a set of atomic values/registers). The sector has a size of 4096 bytes.

As follows from the above description, a complete Atomic Disk Drive consists of two parts: a Linux block device driver, and a user-space library implementing the distributed register. The Linux block device driver is provided in the package (see instructions on how to use it), and you can use it to test your solution. Your task is to implement in Rust the user-space part as the distributed system.

## Distributed register

Your implementation of the distributed register shall be based on an algorithm named (N, N)-AtomicRegister.
(N, N)-AtomicRegister

There is a fixed number of instances of the AtomicRegister module, N, and all instances know about each other. Crashes of individual instances can happen. Every instance can initiate both read and write operations (thus the (N, N) in the name of the algorithm). It is assumed that the system is able to progress on operations as long as at least the majority of the instances are working correctly.

The core algorithm, based on the Reliable and Secure Distributed Programming by C. Cachin, R. Guerraoui, L. Rodrigues and modified to suit crash-recovery model, is as follows:

Implements:
    (N,N)-AtomicRegister instance nnar.

Uses:
    StubbornBestEffortBroadcast, instance sbeb;
    StubbornLinks, instance sl;

upon event < nnar, Init > do
    (ts, wr, val) := (0, 0, _);
    rid:= 0;
    readlist := [ _ ] `of length` N;
    acklist := [ _ ] `of length` N;
    reading := FALSE;
    writing := FALSE;
    writeval := _;
    readval := _;
    write_phase := FALSE;
    store(wr, ts, val, rid, writing, writeval);

upon event < nnar, Recovery > do
    retrieve(wr, ts, val, rid, writing, writeval);
    readlist := [ _ ] `of length` N;
    acklist := [ _ ]  `of length` N;
    reading := FALSE;
    readval := _;
    write_phase := FALSE;
    writing := FALSE;if writing = TRUE then
    writeval := _;    trigger < sbeb, Broadcast | [READ_PROC, rid] >;

upon event < nnar, Read > do
    rid := rid + 1;
    store(rid);
    readlist := [ _ ] `of length` N;
    acklist := [ _ ] `of length` N;
    reading := TRUE;
    trigger < sbeb, Broadcast | [READ_PROC, rid] >;

upon event < sbeb, Deliver | p [READ_PROC, r] > do
    trigger < sl, Send | p, [VALUE, r, ts, wr, val] >;

upon event <sl, Deliver | q, [VALUE, r, ts', wr', v'] > such that r == rid and !write_phase do
    readlist[q] := (ts', wr', v');
    if #(readlist) > N / 2 and (reading or writing) then
        readlist[self] := (ts, wr, val);
        (maxts, rr, readval) := highest(readlist);
        readlist := [ _ ] `of length` N;
        acklist := [ _ ] `of length` N;
        write_phase := TRUE;
        if reading = TRUE then
            trigger < sbeb, Broadcast | [WRITE_PROC, rid, maxts, rr, readval] >;
        else
            (ts, wr, val) := (maxts + 1, rank(self), writeval);
            store(ts, wr, val);
            trigger < sbeb, Broadcast | [WRITE_PROC, rid, maxts + 1, rank(self), writeval] >;

upon event < nnar, Write | v > do
    rid := rid + 1;
    writeval := v;
    acklist := [ _ ] `of length` N;
    readlist := [ _ ] `of length` N;
    writing := TRUE;
    store(rid, writeval, writing);
    trigger < sbeb, Broadcast | [READ_PROC, rid] >;

upon event < sbeb, Deliver | p, [WRITE_PROC, r, ts', wr', v'] > do
    if (ts', wr') > (ts, wr) then
        (ts, wr, val) := (ts', wr', v');
        store(ts, wr, val);
    trigger < sl, Send | p, [ACK, r] >;

upon event < sl, Deliver | q, [ACK, r] > such that r == rid and write_phase do
    acklist[q] := Ack;
    if #(acklist) > N / 2 and (reading or writing) then
        acklist := [ _ ] `of length` N;
        write_phase := FALSE;
        if reading = TRUE then
            reading := FALSE;
            trigger < nnar, ReadReturn | readval >;
        else
            writing := FALSE;
            trigger < nnar, WriteReturn >;

The rank(*) returns a rank of an instance, which is a static number assigned to an instance. The highest(*) returns the largest value ordered by (timestamp, rank).

Your solution will not be receiving special Recovery or Init events. Each time it starts, it shall try to recover from its stable storage (during the initial run, the stable storage will be empty). Crashes are expected to happen at any point, your solution shall work despite them. The algorithm presented above is only a pseudocode, so we suggest understanding ideas behind it.

## Linearization

Usually, components of a distributed system do not share a common clock. Atomic Disk Device does not have one too, and thus the events can happen at different rates and in various orders in every process. However, the atomic register enforces constraints between events on processes, and thereby it makes it possible to put all read and write operations on a single timeline, and to mark start and end of each operation. Every read returns the most recently written value. If an operation o happens before operation o' when the system is processing messages, then o must appear before o' on a such common timeline. This is called linearization.

To sum up, from a perspective of as single client (e.g., the Linux driver), there is a single sequence of read and write events. The client would not able to distinguish between the distributed register and some single device if it was performing the operations instead.
Performance

The atomic register algorithm, as presented above, can only progress with one read or write operation at a time. However, this restriction applies only to a single value. Therefore, to improve performance of Atomic Disk Device, one can run multiple instances of the atomic register logic, each progressing on a different sector. It is expected that anything above a bare-minimum solution will provide such kind of concurrency and will be able to process many sectors at once.
