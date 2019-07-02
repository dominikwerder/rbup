use dfail::{fail, Fail};

pub fn scan(path: &std::path::Path) -> Result<(), Fail> {
  #[cfg(all(unix, target_os="linux"))]
  use std::os::linux::fs::MetadataExt;
  #[cfg(all(unix, target_os="macos"))]
  use std::os::macos::fs::MetadataExt;
  use std::os::unix::fs::FileTypeExt;
  for entry in std::fs::read_dir(path)? {
    let entry = entry?;
    entry.metadata()?.st_gen();
    let ty = entry.file_type()?;
    if ty.is_dir() {
      //eprintln!("dir: {:?}", entry);
      scan(&entry.path())?;
    }
    else if ty.is_file() {
      if ty.is_symlink() {
        return Err(fail!("file but is reall symlink"))
      }
      let s = file_info_string(&entry.path())?;
      println!("{}", s);
    }
    else if ty.is_char_device() { }
    else if ty.is_block_device() { }
    else if ty.is_fifo() { }
    else if ty.is_socket() { }
    else if ty.is_symlink() { }
    else { return Err(fail!("unknown type: {:?}", ty)) }
  }
  Ok(())
}

fn file_info_string(path: &std::path::Path) -> Result<String, Fail> {
  use std::io::Read;
  use sha1::{Sha1, Digest};
  let mut f = std::fs::File::open(path)?;
  let mut hasher = Sha1::new();
  let mut buf = [0u8; 4 * 1024 * 1024];
  let mut len = 0;
  loop {
    let n = f.read(&mut buf)?;
    if n == 0 {
      break;
    }
    hasher.input(&buf[..n]);
    len += n;
  }
  let pathstr = match path.to_str() {
    Some(x) => format!("s {:?}", x),
    None => {
      format!("o {:?}", path.as_os_str())
    }
  };
  Ok(format!("{}  {:13}  {}", hex::encode(hasher.result()), len, pathstr))
}

#[test] fn weird_pathname_compatibility() {
  if true { return; }
  // TODO
  use std::io::Write;
  let mut f1 = std::fs::File::create("t1/\x02some\x01bad\x05path").unwrap();
  f1.write(b"Yes").unwrap();
  drop(f1);
}

#[derive(Debug)]
pub struct FileSizeHash {
  size: usize,
  sha1: [u8; 20],
  path: std::path::PathBuf,
}

pub fn parse_scandata(reader: &mut std::io::Read) -> Vec<FileSizeHash> {
  let mut ret = vec![];
  use std::io::BufRead;
  let rx = regex_line();
  let rx2 = regex_line2();
  for line in std::io::BufReader::new(reader).lines() {
    let line = line.unwrap();
    let size;
    let sha1;
    let path;
    // TODO massage the path depending on 's' or 'o' flag, and interpret escape sequences...
    if let Some(cap) = rx2.captures(&line) {
      sha1 = cap.get(1).unwrap().as_str();
      size = cap.get(2).unwrap().as_str().parse().unwrap();
      path = cap.get(4).unwrap().as_str().into()
    }
    else if let Some(cap) = rx.captures(&line) {
      sha1 = cap.get(1).unwrap().as_str();
      size = 0;
      path = cap.get(4).unwrap().as_str().into()
    }
    else {
      panic!("no match: {:?}", line);
    }
    let sha1 = hex::decode(sha1).unwrap();
    const N: usize = 20;
    // safety check for the transmute below
    if sha1.len() != N {
      panic!("expected {} byte sha1 hex encoded, got: {:?}", N, sha1);
    }
    let x = FileSizeHash {
      size,
      sha1: * unsafe {
        // size manually checked above
        std::mem::transmute::<_, &[u8; N]>(sha1.first().unwrap())
      },
      path,
    };
    ret.push(x);
  }
  ret
}

fn regex_line() -> regex::Regex {
  regex::Regex::new("([[:alnum:]]+)\\s+(s|o)\\s+\"(([^\\\\\"]*|\\\\\"|\\\\)*)*\"").unwrap()
}

fn regex_line2() -> regex::Regex {
  regex::Regex::new("([[:alnum:]]+)\\s+([0-9]+)\\s+(s|o)\\s+\"(([^\\\\\"]*|\\\\\"|\\\\)*)*\"").unwrap()
}

#[test] fn escaped_strings_simple() {
  let rx = regex_line();
  let cap = rx.captures("hash123 s \"filename\"").unwrap();
  assert_eq!(cap.get(1).unwrap().as_str(), "hash123");
  assert_eq!(cap.get(2).unwrap().as_str(), "s");
  assert_eq!(cap.get(3).unwrap().as_str(), "filename");
}

#[test] fn escaped_strings_quot() {
  let rx = regex_line();
  let cap = rx.captures("4242 o \"filename escaped \\\" quot\"").unwrap();
  assert_eq!(cap.get(1).unwrap().as_str(), "4242");
  assert_eq!(cap.get(2).unwrap().as_str(), "o");
  assert_eq!(cap.get(3).unwrap().as_str(), "filename escaped \\\" quot");
}

pub fn check_sha1_in_bup(scanlist: &std::path::Path, bupdir: &std::path::Path) -> Result<(), Fail> {
  // TODO
  // Compute the list of contained file hashes first!
  #[allow(unused)]
  let v1 = parse_scandata(&mut std::fs::File::open(scanlist).unwrap());
  let odb = crate::rbup::Odb::open(bupdir.into())?;
  odb.for_each_obj(|obj| {
    if true { unimplemented!("   TODO   "); }
    /*
    • Parse the tree
    • Discover files: .bup .bupl .bupm or plain directory
    • Do blobs have any more information?
    */
    if obj.is_tree() {
      eprintln!("tree..... {}", obj.oid());
    }
    Err(fail!("unimplemented"))
  })?;
  return Err(fail!("unimplemented"));
}

#[test] fn parse_example() {
  let v1 = parse_scandata(&mut std::fs::File::open("/Users/dwerder/s7-sd-after-fail.txt").unwrap());
  assert!(v1.len() > 500);
}
