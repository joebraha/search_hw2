use std::{
    collections::{self, HashMap},
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    time,
};

const FILE_PATH: &str = "collection.tsv";

fn main() {
    let mut count = 0;
    let start = time::SystemTime::now();
    let mut docs = DocTable::new();
    let mut words = WordTable::new();
    let mut postings = Postings::new();

    let f = File::open(FILE_PATH).unwrap();
    let reader = BufReader::new(f);
    let mut fdocs = BufWriter::new(File::create("docs_out.txt").unwrap());
    let mut fposts = BufWriter::new(File::create("posts_out.txt").unwrap());
    let mut fwords = BufWriter::new(File::create("words_out.txt").unwrap());

    let lines = reader.lines();
    for line in lines {
        let line = line.unwrap();
        let (doctable_index, line) = insert_doc(line, &mut docs);
        parse(line, &mut docs, &mut postings, &mut words);
        count += 1;
        if count > 1000000 {
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

    // pipe out the last remaining data
    pipe_posts(&mut postings, &mut fposts);
    pipe_docs(&mut docs, &mut fdocs);
    // pipe out the whole word table
    pipe_words(&mut words, &mut fwords);
}

fn pipe_posts(postings: &mut Postings, fposts: &mut BufWriter<File>) {
    for post in &postings.posts {
        fposts
            .write(format!("{} {} {}\n", post.word, post.docid, post.count).as_bytes())
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

fn parse(line: String, docs: &mut DocTable, postings: &mut Postings, words: &mut WordTable) {
    // replaced delimiters with is_ascii_punctuation. More aggressive parse, which should result in
    // better real english words, at the (acceptable) expense of tokenizing ranodm words (math etc.)
    let line = line.split(|c| char::is_whitespace(c) || char::is_ascii_punctuation(&c));
    let mut twords = HashMap::new();
    let mut doc_count = 0;
    for word in line {
        let word = word.to_lowercase();
        doc_count += 1;
        // skip non ascii
        if word.contains(|c| !char::is_ascii(&c)) {
            continue;
        }
        // remove all non-ascii, to aggresively target normal English words
        let word: String = word.chars().filter(|&c| c >= 'a' && c <= 'z').collect();
        // attempt to skip empty words
        if !word.contains(|c| c >= 'a' && c <= 'z') {
            continue;
        }
        match twords.get_mut(&word) {
            Some(c) => *c += 1,
            None => {
                twords.insert(word, 1);
            }
        }
    }
    docs.set_last_length(doc_count);
    for (word, count) in twords.into_iter() {
        words.add(&word);
        postings.add(word, count, docs.last_id());
    }
}

// now with the ""collection" database doc structure is differnet, but this should still work
struct DocTable {
    docs: Vec<(String, u32)>,
}

impl DocTable {
    fn new() -> Self {
        return DocTable { docs: Vec::new() };
    }

    fn add(&mut self, doc: String) -> usize {
        self.docs.push((doc, 0));
        return self.docs.len() - 1;
    }

    // sets length of last entry in doctable
    fn set_last_length(&mut self, length: u32) {
        let ind = self.docs.len() - 1;
        self.docs[ind].1 = length;
    }

    fn last_id(&self) -> String {
        let ind = self.docs.len() - 1;
        self.docs[ind].0.to_string()
    }
}

struct WordTable {
    words: collections::HashMap<String, u32>,
}

impl WordTable {
    fn new() -> Self {
        return Self {
            words: HashMap::new(),
        };
    }
    fn add(&mut self, word: &str) {
        match self.words.get_mut(word) {
            Some(c) => *c += 1,
            None => {
                self.words.insert(word.to_string(), 1);
            }
        }
    }
}

struct Postings {
    posts: Vec<Posting>,
}

impl Postings {
    fn new() -> Self {
        Self { posts: Vec::new() }
    }

    fn add(&mut self, word: String, count: u32, docid: String) {
        self.posts.push(Posting { word, docid, count });
    }
}

struct Posting {
    word: String,
    docid: String,
    count: u32,
}
