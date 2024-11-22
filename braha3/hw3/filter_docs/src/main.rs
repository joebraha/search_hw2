use atoi;
use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        println!("Usage: filter <collection> <docids>.")
    }
    let collection = BufReader::new(File::open(&args[1]).unwrap()).lines();
    let docids = BufReader::new(File::open(&args[2]).unwrap()).lines();
    let mut out = File::create("filtered_docs").unwrap();

    let mut is: Vec<u32> = docids
        .map(|l| atoi::atoi(l.unwrap().as_ref()).unwrap())
        .collect();
    is.sort();
    let mut inds = is.iter();
    println!("{} {}", is.len(), is[1]);
    let mut curr: u32 = inds.next().unwrap().clone();

    for line in collection {
        let line = line.unwrap();
        let a: Vec<&str> = line.split_whitespace().collect();
        // println!("{a:?}");
        if atoi::atoi::<u32>(a[0].as_ref()).unwrap() == curr {
            out.write(format!("{}\n", line).as_ref()).unwrap();
            curr = match inds.next() {
                Some(&i) => i,
                None => return,
            }
        }
    }
}
