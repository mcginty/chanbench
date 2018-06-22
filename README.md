# current results on my laptop

```
naïve single thread: 0.751s 353MB 470.0MB/s
tokio_threadpool: 1.704s 353MB 207.1MB/s
futures_cpupool: 1.039s 353MB 339.8MB/s
rayon chunk: 1.845s 353MB 191.3MB/s
rayon ideal: 0.164s 353MB 2149.8MB/s
crossbeam-channel: 1.457s 353MB 242.3MB/s
```
