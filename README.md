# BlockStorageFilters-IOSTACK
Repository for sample and under development prototypes of filters for Konnector - IOSTACK EU Project

We had used libtree from Franck Bui-Huu fbuihuu at gmail.com

## Available filters

### Mockup Filter
Filter that generates latency (usleep) per block read or written and generates CPU load (number of iterations) per block read or written.

It is used to test the framework and measure overhead, does not modify the contents.

### Prefetch filter
Filter divided in two components _prefetch_ and _prefetch2_, the first one generates a log file with the offset and sizes accessed to /tmp/prefetchlist inside the VM. The second one, preloads the blocks used previously into the filter memory. It is a preliminary version, that will be improved into a Just-in-Time prefetcher. It was the first filter that needed a change in the framework, due to that some blocks are read in advance. The method _preread_ was included for that, it is called by the framework before the read and it returns a *doread* boolean that indicates if we need the normal read to the original device or not.

The prefetch filter drops the block once it is used or modified by a write.

### Deduplicated cache filter
The filter stores in a red-black-tree cached blocks, and apply deduplication to reduce the content. The cache memory is increased on deduplicable workloads, so we may have less accesses to the disk. The filter has several improvements in order to reduce overhead, for example, it is threaded. 
It is not a persistent filter.

The implementation uses the block contents as hash, as memcmp does shortcuts, it is an effective method to hash blocks in memory. The implementation has several methods to remove old blocks (when new blocks does not space to be stored).

### Compressed cache filter
The filter stores in a red-black-tree cached blocks, and apply compression to reduce the content. The cache memory is increased on compressible workloads, so we may have less accesses to the disk. The filter has several improvements in order to reduce overhead, for example, it is threaded. 
It is not a persistent filter.


### Output compressed filter
The filter generates a new compressed file system on the persistent device. The compression is done with LZOP, that is very lightweight, but if the block is not compressible by LZOP it tries ZLIB. This behaviour is configurable with some defines. 
Actually, only CACHE, and BEST are working without issues. All the threaded behaviour is deactivated due to timeouts on the filter framework and bugs, on the other hand it does not produce noticiable benefits compared to the single threaded behaviour.


