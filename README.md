# youtube-dl-wrapper
Build an entire music collection from a list of youtube channels, as quickly and efficiently as possible.

### Problem Definition / "Why Rust?"

Given the limitations of Internet bandwidth, the process of building an entire music collection is lengthy, wallclockwise.

Thus, for this task it is not desirable to use the same computer that is used interactively on a day-to-day basis - that computer might be mobile, it might be beset by some misbehaving program that hogs its resources and necessitates a reboot, etc.

Rather, it is desirable to relegate this task to a small, low-power computer that can be left in a dark corner for awhile.

Under this circumstance, there are several limited resources: CPU, RAM, network bandwidth and my sanity. The sinks of sanity and bandwidth are obvious; RAM is what youtube-dl sometimes eats for breakfast if you point it at a really big youtube channel; and CPU is what ffmpeg eats for breakfast - one core per invocation - when you ask it to extract the audio out of a video and plop the results into a sensible container.

youtube-dl is very much like a multi-cycle CPU - it finds metadata for one item, downloads one item, runs ffmpeg on the one item, then moves on.

So using youtube-dl directly, even with all the batch-processing affordances of its CLI, is a poor use of resources - at any one time, either the network or the CPU lays almost-idle, and the network is never utilized for more than one download at a time, even if you tell youtube-dl to use aria2 instead of its built-in HTTP client.

Using a very short shellscript, I could run about `$NUM_CORES` instances of youtube-dl at a time, each handed one-quarter of the list of channels. This is still a poor use of resources - at any one time, each CPU core either sits almost-idle or corresponds to an almost-idle proportion of the network bandwidth, which in turn is never utilized for more than four downloads at a time.

Using an incrementally-longer shellscript, I could run more than `$NUM_CORES` instances of youtube-dl at a time. CPU being limited, the ffmpeg stage is the slowest part of the process, so theoretically the most common condition of the system during a run is for all of the youtube-dl subprocesses to be running ffmpeg. But in practice, as soon as more than `$NUM_CORES` ffmpegs are running, they start to contend for CPU time, context-switch each other in and out, and the whole system's performance goes down the drain.

Using a longer-still shellscript, I could run more than `$NUM_CORES` instances of youtube-dl at a time but with a concurrency limit on the ffmpeg executable. This is still a poor use of resources because, in practice, ffmpeg will take so long that eventually the system will converge onto the state where almost everything is waiting for more CPU to be available for ffmpeg before it can move onto the next item in the playlist - leaving the network idle.

Using a slightly-modified version of the above shellscript, I could run as many instances of youtube-dl at a time as possible, still with the ffmpeg concurrency limit. This is tantamount to giving the kernel a chicken and letting it work it out - spoiler alert, an ARM-based SBC does not give the kernel sufficient resources to bookkeep, in a timely manner, the spawning of so many processes, _and_ it doesn't even apply any backpressure on user-space. Result: the whole system hangs.

Using a more involved, intelligent and truly-pipelined approach - one that can cache downloaded metadata intelligently, use aria2's full capabilities, queue the yet-to-be-ffmpeg'ed items on disk indefinitely, and keep an interruption-journal of the events prior and prerequisite to discovering individual items' IDs - means just using youtube-dl for its scraping capabilities and redoing the rest of its functionality. This in turn means a more powerful programming language than shell - Python or similar. But, RAM and my sanity being limited, the "or similar" direction is all-but-mandated.

Hence, I used Rust here.
