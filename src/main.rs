use serde::{ser::SerializeSeq, Serialize};
use serde_derive::{Deserialize, Serialize};
// use std::thread;
use std::time;
use std::{
    collections::{self, HashMap},
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
};

const FILE_PATH: &str = "fulldocs.tsv";

fn main() {
    let mut count = 0;
    let start = time::SystemTime::now();
    let mut docs = DocTable::new();
    let mut words = WordTable::new();
    let mut postings = Postings::new();

    let f = File::open(FILE_PATH).unwrap();
    let reader = BufReader::new(f);
    let mut fdocs = BufWriter::new(File::create("testout_docs.txt").unwrap());
    let mut fposts = BufWriter::new(File::create("testout_posts.txt").unwrap());
    let mut fwords = BufWriter::new(File::create("testout_words.txt").unwrap());

    let lines = reader.lines();
    for line in lines {
        let line = line.unwrap();
        let (docid, line) = insert_doc(line, &mut docs);
        parse(line, &mut docs, &mut postings, &mut words, docid);
        count += 1;
        if count > 10000 {
            println!(
                "words: {}, docs: {}, posts: {}, time: {}s",
                words.words.keys().len(),
                docs.docs.len(),
                postings.posts.len(),
                time::SystemTime::now()
                    .duration_since(start)
                    .unwrap()
                    .as_secs()
            );
            // let mut threads = vec![];
            pipe_posts(&mut postings, &mut fposts);
            pipe_docs(&mut docs, &mut fdocs);
            count = 0;
        }
    }

    println!(
        "words: {}, docs: {}, posts: {}",
        words.words.keys().len(),
        docs.docs.len(),
        postings.posts.len()
    );

    // TODO: threading
    pipe_posts(&mut postings, &mut fposts);
    pipe_docs(&mut docs, &mut fdocs);
    pipe_words(&mut words, &mut fwords);
}

fn pipe_posts(postings: &mut Postings, fposts: &mut BufWriter<File>) {
    for post in &postings.posts {
        fposts
            // TODO: atoi
            .write(format!("{} {} {}\n", post.word, post.count, post.docid).as_bytes())
            .unwrap();
    }
    postings.posts.clear();
}

fn pipe_words(words: &mut WordTable, fwords: &mut BufWriter<File>) {
    for word in &words.words {
        fwords
            .write(format!("{} {}\n", word.0, word.1).as_bytes())
            .unwrap();
    }
    words.words.clear();
}

fn pipe_docs(docs: &mut DocTable, fdocs: &mut BufWriter<File>) {
    for doc in &docs.docs {
        fdocs
            .write(format!("{} {}\n", doc.0, doc.1).as_bytes())
            .unwrap();
    }
    docs.docs.clear();
}

fn insert_doc(line: String, docs: &mut DocTable) -> (usize, String) {
    let l = line.split_once(char::is_whitespace).unwrap();
    (docs.add(l.0.to_string()), l.1.to_string())
}

fn parse(
    line: String,
    docs: &mut DocTable,
    postings: &mut Postings,
    words: &mut WordTable,
    docid: usize,
) {
    let delimiters = [',', '.', ';', '\'', '"', '?', '!'];
    let line = line.split(|c| char::is_whitespace(c) || delimiters.contains(&c));
    let mut twords = HashMap::new();
    let mut doc_count = 0;
    for word in line {
        let word = word.to_lowercase();
        doc_count += 1;
        match twords.get_mut(&word) {
            Some(c) => *c += 1,
            None => {
                twords.insert(word, 1);
            }
        }
    }
    docs.set_length(docid, doc_count);
    for (word, count) in twords.into_iter() {
        words.add(&word, count);
        postings.add(word, count, docid);
    }
}

#[derive(Deserialize)]
struct DocTable {
    docs: Vec<(String, u32)>,
}

impl Serialize for DocTable {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let m = self.docs.iter().enumerate();
        let mut seq = serializer.serialize_seq(Some(self.docs.len())).unwrap();
        for d in m {
            seq.serialize_element(&d).unwrap();
        }
        seq.end()
    }
}

impl DocTable {
    fn new() -> Self {
        return DocTable { docs: Vec::new() };
    }

    // TODO: convert from usize to normal int type
    fn add(&mut self, doc: String) -> usize {
        self.docs.push((doc, 0));
        return self.docs.len() - 1;
    }

    fn set_length(&mut self, docid: usize, length: u32) {
        self.docs[docid].1 = length
    }
}

#[derive(Serialize)]
struct WordTable {
    words: collections::HashMap<String, u32>,
}

impl WordTable {
    fn new() -> Self {
        return Self {
            words: HashMap::new(),
        };
    }
    fn add(&mut self, word: &str, count: u32) {
        match self.words.get_mut(word) {
            Some(c) => *c += 1,
            None => {
                self.words.insert(word.to_string(), count);
            }
        }
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
}

#[derive(Serialize, Deserialize)]
struct Posting {
    word: String,
    docid: usize,
    count: u32,
}
