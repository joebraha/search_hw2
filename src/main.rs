// use serde::{Deserialize, Serialize};
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::{self, HashMap},
    fs::File,
    io::{BufRead, BufReader},
};

const FILE_PATH: &str = "fulldocs.tsv";

fn main() {
    let mut count = 0;
    let mut docs = DocTable::new();
    let mut words = WordTable::new();
    let mut postings = Postings::new();
    let f = File::open(FILE_PATH).unwrap();
    let reader = BufReader::new(f);
    let lines = reader.lines();
    for line in lines {
        let line = line.unwrap();
        let (docid, line) = insert_doc(line, &mut docs);
        parse(line, &mut postings, &mut words, docid);
        count += 1;
        if count > 10000 {
            // TODO: pipe postings into file so that memory use stays low
            break;
        }
    }
    // TODO: replace with efficient serialiation of data to files
    let s = serde_json::to_string(&words).unwrap();
    println!("{s}");
    let d = serde_json::to_string(&docs).unwrap();
    println!("{d}");
    let d = serde_json::to_string(&postings).unwrap();
    println!("{d}");
}

fn insert_doc(line: String, docs: &mut DocTable) -> (usize, String) {
    let l = line.split_once(char::is_whitespace).unwrap();
    (docs.add(l.0.to_string()), l.1.to_string())
}

fn parse(line: String, postings: &mut Postings, words: &mut WordTable, docid: usize) {
    let delimiters = [',', '.', ';', '\'', '"', '?', '!'];
    let line = line.split(|c| char::is_whitespace(c) || delimiters.contains(&c));
    let mut twords = HashMap::new();
    for word in line {
        match twords.get_mut(word) {
            Some(c) => *c += 1,
            None => {
                twords.insert(word, 1);
            }
        }
    }
    for (word, count) in twords.iter() {
        words.add(word);
        postings.add(word.to_string(), *count, docid);
    }
}

#[derive(Serialize, Deserialize)]
struct DocTable {
    docs: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct WordTable {
    words: collections::HashMap<String, Word>,
}

impl WordTable {
    fn new() -> Self {
        return Self {
            words: HashMap::new(),
        };
    }
    fn add(&mut self, word: &str) {
        match self.words.get_mut(word) {
            Some(w) => w.inc(),
            None => {
                self.words.insert(word.to_string(), Word::new());
            }
        }
    }
}

struct WordCounter {
    words: HashMap<String, u32>,
}

#[serde_with::skip_serializing_none]
#[derive(Serialize, Deserialize)]
struct Word {
    start: Option<u32>,
    end: Option<u32>,
    //    #[serde(skip_serializing)]
    count: u32,
}

impl Word {
    fn new() -> Self {
        return Self {
            start: None,
            end: None,
            count: 1,
        };
    }
    fn inc(&mut self) {
        self.count += 1
    }
}

#[derive(Serialize, Deserialize)]
struct Postings {
    posts: Vec<Posting>,
}

impl Postings {
    fn new() -> Self {
        Self { posts: Vec::new() }
    }

    fn add(&mut self, word: String, count: u32, docid: usize) {
        self.posts.push(Posting { word, docid, count });
    }

    fn write_out(&mut self, file: File) {
        // consumes all the postings and appends them to the passed in file
        todo!();
        rmp_serde::write(self, file)
    }
}

#[derive(Serialize, Deserialize)]
struct Posting {
    word: String,
    docid: usize,
    count: u32,
}

impl DocTable {
    // TODO: convert from usize to normal int type
    fn add(&mut self, doc: String) -> usize {
        self.docs.push(doc);
        return self.docs.len() - 1;
    }

    fn write_out(&self, file: File) {
        todo!()
    }

    fn new() -> Self {
        return DocTable { docs: Vec::new() };
    }
}
