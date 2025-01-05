A log reader, that reads from log files while writer still writes
=================================================================

We have a situation with logging events (somewhere upward of 10 million per day) with a cumulative log size of around 300 GB per day. The log files are stored per day, per month and per year on expensive disks and we are migrating to a much cheaper solution. During migration we have the situation where we cannot turn off the old solution before having verified the new solution, so they run in parallel for a while. This personal project is my tentative "Plan C" for integrating the log capturing frontend with the storage backend, should we run into problems related to forking the log data streams during the handover period.

The log events emanates from a very large Tuxedo cluster (now a Casual cluster) and the log posts are collected from a set of /Q(ueues). From each such queue, for each day, a pair of header- and payload-files are produced. The header file contains various metadata and an offset and a size of binary data stored in the corresponding payload file. The payload file is just a linear dump of binary blobs (payload).

This project contains a fictive log writer (zloggen), for testing purposes, and a log reader (zlogread). 

The log writer acts like a tuxedo handler would do when consuming log events from it's /Q(ueue) and writing to a header- and payload-pair of files. Various stochastic delays are introduced in order to simulate situations 
* where OS-buffers for the individual files are flushed out-of-order, or 
* there are considerable delays in writing so that the files could be flushed with "half-written" posts

The emphasis has really been on the log reader that is consuming the pair of header- and payload-files as they are being written -- with buffering anomalies and all. As an individual day is coming to an end, the log reader needs to be able to follow on over to the new day (another directory) while still being responsive to potential buffering delays. 

The two binaries, of which only the log reader (zlogread) is of interest, branches out into several processes. While you start one instance of zlogread as a monitor, it in turn will start multiple instances of zlogread that processes an individual header- and payload-pair. It is the monitor that handles the number of processors to match the number of header- and payload-pairs.

Also, the log reader can be interrupted at any time and/or restarted as it tracks state; 
* the last header file position successfully read, 
* the last payload file position successfully read, 
* the total accumulated size processed so far (in bytes), 
* the total accumulated count of processed posts so far. 

As it progresses, it will trigger some kind of action, but the action itself is only alluded to (it does some printout). The important bit is that this action is triggered either when a specific number of log posts has been processed or we have reached an accumulated amount of data that will be exceeded if we were to continue -- whatever comes first. In the latter case the action is performed before the last log post is processed.

The code is not particularly nice. This is written in C++ with a kind of C approach -- no classes -- and there is code duplication between the log writer and log reader. This could be cleaned up a bit. 

## Instructing log reader to consume log for one specific day 

What follows is a combined log (from stdout) so individual process logs are mixed.

Starting log reader monitor (monitoring a specific day)
```
➜ ./zlogread . 2024-09-30
[2024-10-01 11:41:44.479004] [debug] Will instantiate sub-processes using executable: "./zlogread"
[2024-10-01 11:41:44.479316] [info] Monitoring directory: "./2024/9/30"
```
The log reader monitor detects header- and payload log file pairs, and spawns individual child processes for processing each pair
```
[2024-10-01 11:41:44.481429] [info] Processor #1 (pid=12238) handles file0.header and file0.payload
[2024-10-01 11:41:44.483158] [info] Processor #2 (pid=12239) handles file1.header and file1.payload
[2024-10-01 11:41:44.484863] [info] Processor #3 (pid=12240) handles file2.header and file2.payload
[2024-10-01 11:41:44.486571] [info] Processor #4 (pid=12242) handles file3.header and file3.payload
[2024-10-01 11:41:44.488317] [info] Processor #5 (pid=12245) handles file4.header and file4.payload
[2024-10-01 11:41:44.490074] [info] Processor #6 (pid=12246) handles file5.header and file5.payload
[2024-10-01 11:41:44.491791] [info] Processor #7 (pid=12247) handles file6.header and file6.payload
[2024-10-01 11:41:44.493504] [info] Processor #8 (pid=12248) handles file7.header and file7.payload
[2024-10-01 11:41:44.495270] [info] Processor #9 (pid=12249) handles file8.header and file8.payload
[2024-10-01 11:41:44.497037] [info] Processor #10 (pid=12250) handles file9.header and file9.payload
```
Each individual child process picks up log file pairs and starts reading from the header file
```
[2024-10-01 11:41:44.505818] [info] Processor #1 starting at position 0 in ./2024/9/30/file0.header
[2024-10-01 11:41:44.509108] [info] Processor #2 starting at position 0 in ./2024/9/30/file1.header
[2024-10-01 11:41:44.509515] [info] Processor #3 starting at position 0 in ./2024/9/30/file2.header
[2024-10-01 11:41:44.512233] [info] Processor #4 starting at position 0 in ./2024/9/30/file3.header
[2024-10-01 11:41:44.514147] [info] Processor #5 starting at position 0 in ./2024/9/30/file4.header
[2024-10-01 11:41:44.516811] [info] Processor #6 starting at position 0 in ./2024/9/30/file5.header
[2024-10-01 11:41:44.518688] [info] Processor #7 starting at position 0 in ./2024/9/30/file6.header
[2024-10-01 11:41:44.520379] [info] Processor #8 starting at position 0 in ./2024/9/30/file7.header
[2024-10-01 11:41:44.522114] [info] Processor #9 starting at position 0 in ./2024/9/30/file8.header
[2024-10-01 11:41:44.526291] [info] Processor #10 starting at position 0 in ./2024/9/30/file9.header
```
One of the header files have been manually doctored to only have half a header (last in file), which makes it impossible for the log reader to process the entry.
All other log reader processes finishes, while the last one struggles for a while.
```
[2024-10-01 11:41:44.556877] [info] Header not ready: file9.header -- Remaining attempts: 10
[2024-10-01 11:41:54.539238] [info] I'm done, since I detected date rollover to 2024-10-01 and I could not read more data from 2024-09-30
[2024-10-01 11:41:54.544344] [info] I'm done, since I detected date rollover to 2024-10-01 and I could not read more data from 2024-09-30
[2024-10-01 11:41:54.544525] [info] Processor #1 (pid=12238) reports: Processed 113 entries
[2024-10-01 11:41:54.548189] [info] I'm done, since I detected date rollover to 2024-10-01 and I could not read more data from 2024-09-30
[2024-10-01 11:41:54.548306] [info] Processor #2 (pid=12239) reports: Processed 99 entries
[2024-10-01 11:41:54.548381] [info] Processor #3 (pid=12240) finished gracefully with report: Processed 83 entries
[2024-10-01 11:41:54.550654] [info] I'm done, since I detected date rollover to 2024-10-01 and I could not read more data from 2024-09-30
[2024-10-01 11:41:54.550777] [info] Processor #4 (pid=12242) reports: Processed 98 entries
[2024-10-01 11:41:54.555777] [info] I'm done, since I detected date rollover to 2024-10-01 and I could not read more data from 2024-09-30
[2024-10-01 11:41:54.555785] [info] I'm done, since I detected date rollover to 2024-10-01 and I could not read more data from 2024-09-30
[2024-10-01 11:41:54.555778] [info] I'm done, since I detected date rollover to 2024-10-01 and I could not read more data from 2024-09-30
[2024-10-01 11:41:54.555967] [info] Processor #5 (pid=12245) reports: Processed 108 entries
[2024-10-01 11:41:54.556028] [info] Processor #6 (pid=12246) reports: Processed 111 entries
[2024-10-01 11:41:54.556812] [trace] Re-attempting header read (10 times)...
[2024-10-01 11:41:54.556820] [info] I'm done, since I detected date rollover to 2024-10-01 and I could not read more data from 2024-09-30
[2024-10-01 11:41:54.556895] [info] Header not ready: file9.header -- Remaining attempts: 9
[2024-10-01 11:41:54.557240] [info] I'm done, since I detected date rollover to 2024-10-01 and I could not read more data from 2024-09-30
[2024-10-01 11:41:54.557355] [info] Processor #7 (pid=12247) reports: Processed 113 entries
[2024-10-01 11:41:54.557410] [info] Processor #8 (pid=12248) finished gracefully with report: Processed 93 entries
[2024-10-01 11:41:54.557467] [info] Processor #9 (pid=12249) finished gracefully with report: Processed 93 entries
[2024-10-01 11:42:04.557317] [trace] Re-attempting header read (9 times)...
[2024-10-01 11:42:04.557471] [info] Header not ready: file9.header -- Remaining attempts: 8
[2024-10-01 11:42:14.557522] [trace] Re-attempting header read (8 times)...
[2024-10-01 11:42:14.557744] [info] Header not ready: file9.header -- Remaining attempts: 7
[2024-10-01 11:42:24.558009] [trace] Re-attempting header read (7 times)...
[2024-10-01 11:42:24.558160] [info] Header not ready: file9.header -- Remaining attempts: 6
[2024-10-01 11:42:34.560308] [trace] Re-attempting header read (6 times)...
[2024-10-01 11:42:34.560444] [info] Header not ready: file9.header -- Remaining attempts: 5
[2024-10-01 11:42:44.560992] [trace] Re-attempting header read (5 times)...
[2024-10-01 11:42:44.561146] [info] Header not ready: file9.header -- Remaining attempts: 4
[2024-10-01 11:42:54.561066] [trace] Re-attempting header read (4 times)...
[2024-10-01 11:42:54.561170] [info] Header not ready: file9.header -- Remaining attempts: 3
[2024-10-01 11:43:04.563861] [trace] Re-attempting header read (3 times)...
[2024-10-01 11:43:04.563989] [info] Header not ready: file9.header -- Remaining attempts: 2
[2024-10-01 11:43:14.564579] [trace] Re-attempting header read (2 times)...
[2024-10-01 11:43:14.564715] [info] Header not ready: file9.header -- Remaining attempts: 1
[2024-10-01 11:43:24.565627] [error] I'm done, since I detected date rollover to 2024-10-01, but I failed repeatedly to read from header file file9.header at offset 4580 in log for 2024-09-30
```
The log monitor picks up the report from the struggling child process
```
[2024-10-01 11:43:24.565999] [info] Processor #10 (pid=12250) reports: Successfully processed 88 entries, but repeatedly failed reading header file file9.header at offset 4580 in log for 2024-09-30
```
...and collects the other child processes as well.
```
[2024-10-01 11:43:24.666976] [info] Processor #1 (pid=12238) finished gracefully with report:
[2024-10-01 11:43:24.667084] [info] Processor #2 (pid=12239) finished gracefully with report:
[2024-10-01 11:43:24.667150] [info] Processor #4 (pid=12242) finished gracefully with report:
[2024-10-01 11:43:24.667193] [info] Processor #5 (pid=12245) finished gracefully with report:
[2024-10-01 11:43:24.667238] [info] Processor #6 (pid=12246) finished gracefully with report:
[2024-10-01 11:43:24.667285] [info] Processor #7 (pid=12247) finished gracefully with report:
[2024-10-01 11:43:24.667328] [error] Processor #10 (pid=12250) could not process all headers in file file9.header.
```
Having tackled 2024-09-30 (somewhat), the log monitor calls it a day.
```
[2024-10-01 11:43:24.771581] [info] Ending
```

