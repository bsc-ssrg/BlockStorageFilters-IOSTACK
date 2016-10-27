# BlockStorageFilters-IOSTACK
Repository for sample and under development prototypes of filters for Konnector - IOSTACK EU Project


## Available filters

### Mockup Filter
Filter that generates latency (usleep) per block read or written and generates CPU load (number of iterations) per block read or written.

It is used to test the framework and measure overhead, does not modify the contents.

### Prefetch filter
Filter divided in two components, the first one generates a log file with the offset and sizes accessed. The second one, preloads the blocks used previously into the filter memory. It is a preliminary version, that will be improved into a Just-in-Time prefetcher. 


### Deduplicated cache filter
The filter stores in a red-black-tree cached blocks, and apply deduplication to reduce the content. The cache memory is increased on deduplicable workloads, so we may have less accesses to the disk. The filter has several improvements in order to reduce overhead, for example, it is threaded. 
It is not a persistent filter.

### Compressed cache filter
The filter stores in a red-black-tree cached blocks, and apply compression to reduce the content. The cache memory is increased on compressible workloads, so we may have less accesses to the disk. The filter has several improvements in order to reduce overhead, for example, it is threaded. 
It is not a persistent filter.


### Output compressed filter
The filter generates a new compressed file system on the persistent device. The compression is done with LZOP, that is very lightweight, but if the block is not compressible by LZOP it tries ZLIB. This behaviour is configurable with some defines. 
Actually, only CACHE, and BEST are working without issues. All the threaded behaviour is deactivated due to timeouts on the filter framework and bugs, on the other hand it does not produce noticiable benefits compared to the single threaded behaviour.


