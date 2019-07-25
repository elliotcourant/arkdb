# Buffers

This package contains some basic helpers for reading and writing arrays
of bytes very quickly and easily while maintaining code readability.

## Allocated Buffers

An allocated buffer is a buffer with a predetermined length.
This buffer cannot be grown or shrank once its been created and is
append only. In that the first byte you write will be placed on the
first item in the byte array, then an offset will be incremented to
place the next byte on the next item.

If you happen to know exactly how long a byte array will be, this
buffer is incredibly efficient and very fast. As benchmarks below
will show.

## Unallocated Buffers

Unallocated buffers are essentially just a friendly wrapper around
a byte array. They simply provide the ability to write clean code
around byte arrays without any real performance benefit.

# Benchmarks.

```
BenchmarkBuffer_Append/allocated_2_bytes-8                   	20000000	      63.9 ns/op	      36 B/op	       3 allocs/op
BenchmarkBuffer_Append/allocated_10_bytes-8                  	10000000	       136 ns/op	      54 B/op	       7 allocs/op
BenchmarkBuffer_Append/allocated_100_bytes-8                 	 2000000	       971 ns/op	     244 B/op	      52 allocs/op
BenchmarkBuffer_Append/allocated_1000_bytes-8                	  200000	      9099 ns/op	    2056 B/op	     502 allocs/op
BenchmarkBuffer_Append/allocated_10000_10_byte_chunk-8         	  500000	      3525 ns/op	   11872 B/op	     102 allocs/op
BenchmarkBuffer_Append/allocated_1000_1000_byte_chunk-8        	10000000	       219 ns/op	    1056 B/op	       2 allocs/op
BenchmarkBuffer_Append/allocated_10000_1000_byte_chunk-8       	 1000000	      1300 ns/op	   10272 B/op	       2 allocs/op
BenchmarkBuffer_Append/un-allocated_2_bytes-8                  	20000000	      79.5 ns/op	      48 B/op	       3 allocs/op
BenchmarkBuffer_Append/un-allocated_10_bytes-8                 	10000000	       184 ns/op	      72 B/op	       8 allocs/op
BenchmarkBuffer_Append/un-allocated_100_bytes-8                	 1000000	      1068 ns/op	     384 B/op	      56 allocs/op
BenchmarkBuffer_Append/un-allocated_1000_bytes-8               	  200000	      9860 ns/op	    3072 B/op	     509 allocs/op
BenchmarkBuffer_Append/un-allocated_10000_10_byte_chunk-8      	  500000	      3344 ns/op	    3664 B/op	     108 allocs/op
BenchmarkBuffer_Append/un-allocated_1000_1000_byte_chunk-8     	 5000000	       310 ns/op	    1056 B/op	       2 allocs/op
BenchmarkBuffer_Append/un-allocated_10000_1000_byte_chunk-8    	  200000	      7199 ns/op	   44320 B/op	       9 allocs/op
```

As you can see using the benchmarks above, the allocated buffer
is always more efficient because the memory that is needed for that data
is already allocated. The memory is just overwritten as data
is added to the buffer.

But because the unallocated buffer must grow the array each time
something is appended, it can be very inefficient.