/// `fingertips` creates an inverted index for a set of text files.
///
/// Most of the actual work is done by the modules `index`, `read`, `write`,
/// and `merge`.  In this file, `main.rs`, we put the pieces together in two
/// different ways.
///
/// *   `run_single_threaded` simply does everything in one thread, in
///     the most straightforward possible way.
///
/// *   Then, we break the work into a five-stage pipeline so that we can run
///     it on multiple CPUs. `run_pipeline` puts the five stages together.
///
/// The `main` function at the end handles command-line arguments. It calls one
/// of the two functions above to do the work.

mod index;
mod read;
mod write;
mod merge;
mod tmp;
mod off_thread_ext;

use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::mpsc::{self, channel, Receiver};
use std::sync::{Arc, Mutex};
use std::{io, thread};

use argparse::{ArgumentParser, Collect, Store, StoreTrue};
use off_thread_ext::AccValue;

use crate::index::InMemoryIndex;
use crate::write::write_index_to_tmp_file;
use crate::merge::FileMerge;
use crate::tmp::TmpDir;
use crate::off_thread_ext::{ConditionAccmulatorExt, ErrorsTo, OffThreadExt};

/// Create an inverted index for the given list of `documents`,
/// storing it in the specified `output_dir`.
fn run_single_threaded(documents: Vec<PathBuf>, output_dir: PathBuf) -> io::Result<()> {
    // If all the documents fit comfortably in memory, we'll create the whole
    // index in memory.
    let mut accumulated_index = InMemoryIndex::new();

    // If not, then as memory fills up, we'll write largeish temporary index
    // files to disk, saving the temporary filenames in `merge` so that later we
    // can merge them all into a single huge file.
    let mut merge = FileMerge::new(&output_dir);

    // A tool for generating temporary filenames.
    let mut tmp_dir = TmpDir::new(&output_dir);

    // For each document in the set...
    for (doc_id, filename) in documents.into_iter().enumerate() {
        // ...load it into memory...
        let mut f = File::open(filename)?;
        let mut text = String::new();
        f.read_to_string(&mut text)?;

        // ...and add its contents to the in-memory `accumulated_index`.
        let index = InMemoryIndex::from_single_document(doc_id, text);
        accumulated_index.merge(index);
        if accumulated_index.is_large() {
            // To avoid running out of memory, dump `accumulated_index` to disk.
            let file = write_index_to_tmp_file(accumulated_index, &mut tmp_dir)?;
            merge.add_file(file)?;
            accumulated_index = InMemoryIndex::new();
        }
    }

    // Done reading documents! Save the last data set to disk, then merge the
    // temporary index files if there are more than one.
    if !accumulated_index.is_empty() {
        let file = write_index_to_tmp_file(accumulated_index, &mut tmp_dir)?;
        merge.add_file(file)?;
    }
    merge.finish()
}

enum Message {
    Text(String),
    FilePath(PathBuf),
}

fn run_pipeline(
    documents: Vec<PathBuf>,
    output_dir: PathBuf,
    error_sender: mpsc::Sender<io::Result<Message>>,
) -> io::Result<()> {
    let sender = Arc::new(Mutex::new(error_sender));
    // ? 这里不能 Clone 是因为 mpsc::sync::IntoIter 不能 Clone
    // ? Map  本身是可以 Clone 的
    // ? Map 本身有实现 Clone 但具体是否可以 Clone 还要看 底层迭代器是否可以 Clone
    let output_dir_copy_one = output_dir.clone();
    let fille_merge_result: Vec<Message> = documents
        .into_iter()
        .map(read_whole_file)
        .errors_to(sender.clone()) // 过滤出错误结果
        .off_thread() // 为上面的工作生成线程;
        .enumerate()
        .map(make_single_file_index)
        .off_thread() // 为第二阶段生成另一个线程
        .filter_map(|result| result.ok())
        .condition_reduce(make_in_memory_merge)
        .filter_map(|item| match item {
            AccValue::Value(item) => Some(item),
            _ => None,
        })
        .off_thread() // 为第三阶段生成另一个线程
        .map(move |big_indexes| {
            let mut tmp_dir = TmpDir::new(&output_dir_copy_one);
            let file = write_index_to_tmp_file(big_indexes, &mut tmp_dir)?;
            Ok(Message::FilePath(file))
        })
        .errors_to(sender.clone())
        .off_thread() // 为第四阶段生成另一个线程
        .collect();

    let merge = FileMerge::new(&output_dir);
    let r = fille_merge_result
        .into_iter()
        .fold(Ok(merge), make_merge_index_file);

    let m = r?;
    let _ = m.finish();
    Ok(())
}

fn make_merge_index_file(merge: io::Result<FileMerge>, message: Message) -> io::Result<FileMerge> {
    if let Message::FilePath(file) = message {
        println!("make_merge_index_file: file: {}", file.display());
        let mut m = merge?;
        let _ = m.add_file(file)?;
        return Ok(m);
    }
    return Err(io::Error::new(
        io::ErrorKind::Other,
        "make_merge_index_file: Message type is incorroct.",
    ));
}

fn make_in_memory_merge<'r>(
    acc: &'r mut InMemoryIndex,
    file_index: Option<InMemoryIndex>,
) -> Option<InMemoryIndex> {
    // scan 可能的解决方案  https://github.com/rust-lang/rust/issues/68371
    // 这里是不会失败的
    let fi = file_index.unwrap();
    acc.merge(fi);
    if acc.is_large() {
        // ! 假设允许的话， 会发生什么？
        // ! 那么本质上是 acc 被引用了， 将会给到最后生成的内容中，但是不停的在修改它，
        // ! 最终得到的容器中的内容，每个元素都是一样的。
        // ! 这是不对的，所以 rust 会阻止这种情况的发生。
        // ! 体现出来的错误就是 生命周期 不够长。 而且多个 可变引用 不能同时存在。
        // ! 以及可变引用的 唯一性， 在可变引用的生存期间，不允许有其他的 引用。

        // 内存替换，将src移动到引用的dest中，返回之前的dest值。这两个值都不会被删除。
        // ? 这种方式可能会更高效率， 因为仅仅只是将src移动到dest中，而不进行任何的复制。
        let old_acc = std::mem::replace(acc, InMemoryIndex::new());
        println!(
            "make_in_memory_merge: old_acc: {:?}, acc: {:?}",
            old_acc, acc
        );
        return Some(old_acc);
    }
    // ? 这个地方我想要的效果是 acc 不算累计，直到完成， 如果超出阈值， 才 返回一个结果。 这时候结果是一个
    // ? 数组中存在多个 合并后的 InMemoryIndex 实例。
    // ? 如果没有超出内存阀值，那么则返回一个 合并后的 InMemoryIndex 实例。
    // * 这个问题在我写的 condition_reduce 中处理了 None 的逻辑。
    None
}

fn make_single_file_index((doc_id, text): (usize, Message)) -> io::Result<InMemoryIndex> {
    match text {
        Message::Text(t) => {
            let index = InMemoryIndex::from_single_document(doc_id, t);
            Ok(index)
        }
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Not a text")),
    }
}

fn read_whole_file(filename: PathBuf) -> io::Result<Message> {
    let mut f = File::open(filename)?;
    let mut text = String::new();
    f.read_to_string(&mut text)?;
    Ok(Message::Text(text))
}

/// Given some paths, generate the complete list of text files to index. We check
/// on disk whether the path is the name of a file or a directory; for
/// directories, all .txt files immediately under the directory are indexed.
/// Relative paths are fine.
///
/// It's an error if any of the `args` is not a valid path to an existing file
/// or directory.
fn expand_filename_arguments(args: Vec<String>) -> io::Result<Vec<PathBuf>> {
    let mut filenames = vec![];
    for arg in args {
        let path = PathBuf::from(arg);
        if path.metadata()?.is_dir() {
            for entry in path.read_dir()? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    filenames.push(entry.path());
                }
            }
        } else {
            filenames.push(path);
        }
    }
    Ok(filenames)
}

/// Generate an index for a bunch of text files.
fn run(filenames: Vec<String>, single_threaded: bool, output_dir: PathBuf) -> io::Result<()> {
    let documents = expand_filename_arguments(filenames)?;
    if single_threaded {
        run_single_threaded(documents, output_dir)
    } else {
        let (error_sender, error_receiver) = channel::<io::Result<Message>>();
        let _ = run_pipeline(documents, output_dir, error_sender);
        error_handler(error_receiver);
        Ok(())
    }
}

// 下面开始编写错误处理的代码
fn error_handler(error_receiver: Receiver<io::Result<Message>>) {
    let _ = thread::spawn(move || {
        for result in error_receiver {
            match result {
                Err(error) => {
                    println!("Error reading file: {}", error);
                }
                _ => {}
            }
        }
    })
    .join();
}

fn main() {
    let mut single_threaded = false;
    let mut filenames = vec!["./test-files".to_string()];
    let mut output_dir = PathBuf::from(".");

    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Make an inverted index for searching documents.");
        ap.refer(&mut single_threaded).add_option(
            &["-1", "--single-threaded"],
            StoreTrue,
            "Do all the work on a single thread.",
        );
        ap.refer(&mut filenames).add_argument(
            "filenames",
            Collect,
            "Names of files/directories to index. \
                           For directories, all .txt files immediately \
                           under the directory are indexed.",
        );
        ap.refer(&mut output_dir).add_option(
            &["-o", "--output_dir"],
            Store,
            "Directory to write the index to path, default is current directory.",
        );
        ap.parse_args_or_exit();
    }

    match run(filenames, single_threaded, output_dir) {
        Ok(()) => {}
        Err(err) => println!("error: {}", err),
    }
}
