#[macro_use] extern crate log;
extern crate crossbeam_channel;
extern crate futures;
extern crate futures_cpupool;
extern crate num_cpus;
extern crate tokio_core;
extern crate tokio_threadpool;
extern crate rayon;
extern crate snow;

use futures::{sync, unsync};
use futures::{Sink, Stream};
use futures::future::*;
use futures_cpupool::CpuPool;
use rayon::prelude::*;
use snow::NoiseBuilder;
use std::iter;
use std::time::Instant;
use std::thread;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_core::reactor::Core;
use tokio_threadpool::ThreadPool;

const MESSAGE_COUNT : usize = 250_000;
const BUF_SIZE      : usize = 1500;
const CHUNK_SIZE    : usize = 2;

fn main() {
    let pattern = "Noise_NN_25519_ChaChaPoly_BLAKE2b";
    let mut h_i = NoiseBuilder::new(pattern.parse().unwrap())
        .build_initiator().unwrap();
    let mut h_r = NoiseBuilder::new(pattern.parse().unwrap())
        .build_responder().unwrap();

    let mut buffer_msg = vec![0u8; BUF_SIZE];
    let mut buffer_out = vec![0u8; BUF_SIZE];

    // get the handshaking out of the way for even testing
    let len = h_i.write_message(&[0u8; 0], &mut buffer_msg).unwrap();
    h_r.read_message(&buffer_msg[..len], &mut buffer_out).unwrap();
    let len = h_r.write_message(&[0u8; 0], &mut buffer_msg).unwrap();
    h_i.read_message(&buffer_msg[..len], &mut buffer_out).unwrap();

    let mut h_i = h_i.into_async_transport_mode().unwrap();
    let mut h_r = h_r.into_async_transport_mode().unwrap();

    let t_i = h_i.get_async_transport_state().unwrap();
    let t_r = h_r.get_async_transport_state().unwrap();

    assert_eq!(1500, t_i.write_transport_message(1, &[1u8; BUF_SIZE - 16], &mut buffer_msg).unwrap());

    ///////////
    // naïve //
    ///////////
    let mut core = Core::new().unwrap();
    let bytes = AtomicUsize::new(0);
    let (tx, rx) = unsync::mpsc::unbounded::<Vec<u8>>();
    let inmsg = buffer_msg.clone();
    let transport = t_r.clone();
    let rxfut = rx
        .for_each(|decrypted| {
            bytes.fetch_add(decrypted.len(), Ordering::SeqCst);
            Ok(())
        })
        .map_err(|_| ());
    let test = lazy(move || {
        let messages = iter::repeat(inmsg).take(MESSAGE_COUNT);

        for message in messages {
            let transport = transport.clone();
            let mut newvec = vec![0u8; BUF_SIZE - 16];
            transport.read_transport_message(1, &message, &mut newvec).unwrap(); 
            tx.unbounded_send(newvec).unwrap();
        }
        Ok(())
    });

    let start = Instant::now();
    core.run(test.join(rxfut)).unwrap();
    let time = Instant::now() - start;
    let time = time.as_secs() as f64 + time.subsec_nanos() as f64 * 1e-9;
    let bytes = bytes.load(Ordering::Relaxed);
    let mbytes = bytes / (1 << 20);
    println!("naïve single thread: {:.3}s {}MB {:.1}MB/s", time, mbytes, mbytes as f64 / time);

    //////////////////////
    // tokio_threadpool //
    //////////////////////

    let mut core = Core::new().unwrap();
    let bytes = AtomicUsize::new(0);
    let (tx, rx) = sync::mpsc::channel::<Vec<u8>>(1024);
    let inmsg = buffer_msg.clone();
    let transport = t_r.clone();
    let rxfut = rx
        .map_err(|e| warn!("{:?}", e))
        .for_each(|decrypted| {
            bytes.fetch_add(decrypted.len(), Ordering::SeqCst);
            Ok(())
       });
 
    let test = lazy(move || {
        let pool = ThreadPool::new();
        let messages = iter::repeat(inmsg).take(MESSAGE_COUNT);

        for message in messages {
            let tx = tx.clone();
            let transport = transport.clone();
            pool.spawn(lazy(move || {
                let mut newvec = vec![0u8; BUF_SIZE - 16];
                transport.read_transport_message(1, &message, &mut newvec).unwrap(); 
                Ok(newvec)
            }).and_then(|newvec| {
                tx.send(newvec).map_err(|e| warn!("send err: {:?}", e))
            }).and_then(|_| Ok(()))
            );
        }

        pool.shutdown();
        Ok(())
    });

    let start = Instant::now();
    core.run(test.join(rxfut)).unwrap();
    let time = Instant::now() - start;
    let time = time.as_secs() as f64 + time.subsec_nanos() as f64 * 1e-9;
    let bytes = bytes.load(Ordering::Relaxed);
    let mbytes = bytes / (1 << 20);
    println!("tokio_threadpool: {:.3}s {}MB {:.1}MB/s", time, mbytes, mbytes as f64 / time);

    /////////////////////
    // futures_cpupool //
    /////////////////////

    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let bytes = AtomicUsize::new(0);
    let (tx, rx) = unsync::mpsc::channel::<Vec<Vec<u8>>>(1);
    let inmsg = buffer_msg.clone();
    let transport = t_r.clone();
    let rxfut = rx
        .map_err(|e| warn!("{:?}", e))
        .for_each(|chunk| {
            for decrypted in chunk {
                bytes.fetch_add(decrypted.len(), Ordering::SeqCst);
            }
            Ok(())
        });

    let test = lazy(move || {
        let pool = CpuPool::new_num_cpus();
        let chunk = vec![inmsg; CHUNK_SIZE];
        let chunks = iter::repeat(chunk).take(MESSAGE_COUNT / CHUNK_SIZE);

        for chunk in chunks {
            let tx = tx.clone();
            let transport = transport.clone();
            let fut = pool.spawn(lazy(move || {
                let mut newvecs = vec![];
                for message in chunk {
                    let mut newvec = vec![0u8; BUF_SIZE - 16];
                    transport.read_transport_message(1, &message, &mut newvec).unwrap(); 
                    newvecs.push(newvec);
                }
                Ok(newvecs)
            })).and_then(|newvec| {
                tx.send(newvec).map_err(|e| warn!("send err: {:?}", e))
            }).and_then(|_| Ok(()));
            
            handle.spawn(fut);
        }
        Ok(())
    });

    let start = Instant::now();
    core.run(test.join(rxfut)).unwrap();
    let time = Instant::now() - start;
    let time = time.as_secs() as f64 + time.subsec_nanos() as f64 * 1e-9;
    let bytes = bytes.load(Ordering::Relaxed);
    let mbytes = bytes / (1 << 20);
    println!("futures_cpupool: {:.3}s {}MB {:.1}MB/s", time, mbytes, mbytes as f64 / time);

    ///////////////////
    // rayon chunked //
    ///////////////////

    let mut core = Core::new().unwrap();
    let bytes = AtomicUsize::new(0);
    let inmsg = buffer_msg.clone();
    let transport = t_r.clone();

    let test = lazy(|| -> Result<(), ()> {

        for _ in 0..(MESSAGE_COUNT / CHUNK_SIZE) {
            let messages = rayon::iter::repeatn(inmsg.clone(), CHUNK_SIZE);

            let sum = messages.map(|message| {
                let mut newvec = vec![0u8; BUF_SIZE - 16];
                transport.read_transport_message(1, &message, &mut newvec).unwrap()
            }).sum();
            bytes.fetch_add(sum, Ordering::SeqCst);
        }

        Ok(())
    });

    let start = Instant::now();
    core.run(test).unwrap();
    let time = Instant::now() - start;
    let time = time.as_secs() as f64 + time.subsec_nanos() as f64 * 1e-9;
    let bytes = bytes.load(Ordering::Relaxed);
    let mbytes = bytes / (1 << 20);
    println!("rayon chunk: {:.3}s {}MB {:.1}MB/s", time, mbytes, mbytes as f64 / time);

    /////////////////
    // rayon ideal //
    /////////////////

    let mut core = Core::new().unwrap();
    let bytes = AtomicUsize::new(0);
    let inmsg = buffer_msg.clone();
    let transport = t_r.clone();

    let test = lazy(|| -> Result<(), ()> {
        let messages = rayon::iter::repeatn(inmsg, MESSAGE_COUNT);

        let sum = messages.map(|message| {
            let mut newvec = vec![0u8; BUF_SIZE - 16];
            transport.read_transport_message(1, &message, &mut newvec).unwrap()
        }).sum();
        bytes.fetch_add(sum, Ordering::SeqCst);

        Ok(())
    });

    let start = Instant::now();
    core.run(test).unwrap();
    let time = Instant::now() - start;
    let time = time.as_secs() as f64 + time.subsec_nanos() as f64 * 1e-9;
    let bytes = bytes.load(Ordering::Relaxed);
    let mbytes = bytes / (1 << 20);
    println!("rayon ideal: {:.3}s {}MB {:.1}MB/s", time, mbytes, mbytes as f64 / time);

    ///////////////////////
    // crossbeam-channel //
    ///////////////////////
    let threads            = num_cpus::get(); // One thread for I/O.
    let (sender, receiver) = crossbeam_channel::bounded::<Option<Vec<u8>>>(4096);
    let rx = {
        let (tx, rx) = sync::mpsc::unbounded::<Vec<u8>>();
        debug!("spinning up a crypto pool with {} threads", threads);
        for _ in 0..threads {
            let rx = receiver.clone();
            let transport = transport.clone();
            let mut tx = tx.clone();
            thread::spawn(move || {
                loop {
                    if let Some(Some(message)) = rx.recv() {
                        let mut newvec = vec![0u8; BUF_SIZE - 16];
                        transport.read_transport_message(1, &message, &mut newvec).unwrap();
                        tx.unbounded_send(newvec).unwrap();
                    } else {
                        return;
                    }
                }
            });
        }
        rx
    };

    let mut core = Core::new().unwrap();
    let bytes = AtomicUsize::new(0);
    let inmsg = buffer_msg.clone();

    let test = lazy(|| -> Result<(), ()> {
        let messages = iter::repeat(inmsg).take(MESSAGE_COUNT);

        for message in messages {
            sender.clone().send(Some(message));
        }

        // I'm sure there's a better way to do this.
        for _ in 0..threads {
            sender.clone().send(None);
        }

        Ok(())
    });

    let rxfut = rx
        .map_err(|e| warn!("{:?}", e))
        .for_each(|decrypted| {
            bytes.fetch_add(decrypted.len(), Ordering::SeqCst);
            Ok(())
       });

    let start = Instant::now();
    core.run(rxfut.join(test)).unwrap();
    let time = Instant::now() - start;
    let time = time.as_secs() as f64 + time.subsec_nanos() as f64 * 1e-9;
    let bytes = bytes.load(Ordering::Relaxed);
    let mbytes = bytes / (1 << 20);
    println!("crossbeam-channel: {:.3}s {}MB {:.1}MB/s", time, mbytes, mbytes as f64 / time);
}
