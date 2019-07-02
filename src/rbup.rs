use dfail::{Fail, fail, faildb};
use git2::{Repository, ObjectType, Object};
use sha1::{Sha1, Digest};
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::mem::transmute;
use std::cell::{RefCell, RefMut};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::ops::DerefMut;

fn seek(file: &mut ReadSeek, pos: SeekFrom) -> std::io::Result<u64> {
  if true {
    std::thread::sleep(std::time::Duration::from_micros(1000));
  }
  file.seek(pos)
}

pub fn explore(bupdir: &std::path::Path) -> Result<(), String> {
  let mut wr = std::io::BufWriter::new(std::io::stdout());
  let mut total_bytes = 0;
  let mut total_files = 0;
  let repo = Repository::open(bupdir).unwrap();
  repo.odb().unwrap().foreach(|x| {
    let (_size, oty) = repo.odb().unwrap().read_header(*x).unwrap();
    if oty == ObjectType::Commit {
      //eprintln!("Commit: {:?}", x);
      let commit = repo.find_commit(x.clone()).unwrap();
      explore_tree(&mut total_bytes, &mut total_files, &mut wr, &repo, &commit.tree().unwrap()).unwrap();
    }
    if oty == ObjectType::Tree {
      //eprintln!("Tree: {:?}", x);
    }
    // Return true to indicate that we want to continue
    true
  }).unwrap();
  Ok(())
}

fn explore_tree(total_bytes: &mut usize, total_files: &mut usize, wr: &mut std::io::Write, repo: &Repository, tree: &git2::Tree) -> Result<(), Fail> {
  //eprintln!("Exploring tree: {:?}", tree);
  for e in tree.iter() {
    //eprintln!("{:?}  {:?}    {}", e.kind(), e.name(), e.id());
    if let Some(ty) = e.kind() {
      if let Some(name) = e.name() {
        let mut pushed = false;
        if ty == ObjectType::Tree {
          if name.ends_with(".bupl") {
            // TODO this can also be a directory, must check for a .bupm entry
            let x = file_stats(repo, &name[..name.len()-5], &e.to_object(repo).unwrap());
            *total_bytes += x.size;
            *total_files += 1;
            x.write(wr)?;
            pushed = true;
          }
          else if name.ends_with(".bup") {
            // TODO unlike the bupl case, I think this can only be a regular file.
            // Directories which don't end with ".bup" are not mangled.
            let x = file_stats(repo, &name[..name.len()-4], &e.to_object(repo).unwrap());
            *total_bytes += x.size;
            *total_files += 1;
            x.write(wr)?;
            pushed = true;
          }
          else {
            explore_tree(total_bytes, total_files, wr, repo, &repo.find_tree(e.id()).unwrap()).unwrap();
          }
        }
        else if ty == ObjectType::Blob {
          if name.ends_with(".bupl") {
            let x = file_stats(repo, &name[..name.len()-5], &e.to_object(repo).unwrap());
            *total_bytes += x.size;
            *total_files += 1;
            x.write(wr)?;
            pushed = true;
          }
          else if name.ends_with(".bup") {
            let x = file_stats(repo, &name[..name.len()-4], &e.to_object(repo).unwrap());
            *total_bytes += x.size;
            *total_files += 1;
            x.write(wr)?;
            pushed = true;
          }
          else if name.ends_with(".bupm") {
            // TODO metadata
          }
          else {
            // a direct file, small enough to fit into one blob
            let x = file_stats(repo, &name, &e.to_object(repo).unwrap());
            *total_bytes += x.size;
            *total_files += 1;
            x.write(wr)?;
            pushed = true;
            //return Err(fail!("still traversing tree, but what is this? {:?} {:?}", e.id(), e.name()))
          }
        }
        else {
          return Err(fail!("not sure what to do:  {:?}   {:?}   {}", e.kind(), e.name(), e.id()))
        }
        if pushed && *total_files % 100 == 0 {
          eprintln!("bup MB: {:6}  n: {:6}", *total_bytes / 1024 / 1024, *total_files);
        }
      }
      else {
        return Err(fail!("TreeEntry without name: {:?}", e.id()))
      }
    }
    else {
      return Err(fail!("TreeEntry without kind: {:?}", e.id()))
    }
  }
  Ok(())
}

pub struct FileSizeHash {
  size: usize,
  sha1: [u8; 20],
}

impl FileSizeHash {
  pub fn sha1(&self) -> &[u8; 20] {
    &self.sha1
  }
  pub fn write(&self, w: &mut std::io::Write) -> std::io::Result<usize> {
    const N: usize = std::mem::size_of::<usize>();
    let b = unsafe { std::mem::transmute::<_, &[u8; N]>(&self.size) };
    Ok(w.write(b)? + w.write(&self.sha1[..])?)
  }
}

#[test] fn usize_is_8byte() {
  assert_eq!(std::mem::size_of::<usize>(), 8);
}

fn file_stats(repo: &Repository, _name: &str, obj: &Object) -> FileSizeHash {
  //eprintln!("found a file: {}", name);
  let mut size = 0;
  let mut hasher = Sha1::new();
  file_hash(repo, &mut size, &mut hasher, obj);
  let sha1 = hasher.result();
  //eprintln!("size: {}   hash: {}", size, hex::encode(sha1));
  const N: usize = 20;
  if sha1.len() != N {
    panic!("expected {} character sha1 hex, got: {:?}", N, sha1);
  }
  let sha1 = * unsafe { std::mem::transmute::<_, &[u8; 20]>(sha1.first().unwrap()) };
  FileSizeHash {
    size,
    sha1,
  }
}

fn file_hash(repo: &Repository, size: &mut usize, hasher: &mut Sha1, obj: &Object) {
  //eprintln!("obj.kind() {:?}", obj.kind());
  if obj.kind().unwrap() == ObjectType::Blob {
    let blob = obj.as_blob().unwrap();
    let content = blob.content();
    *size += content.len();
    hasher.input(content);
  }
  else if obj.kind().unwrap() == ObjectType::Tree {
    for e in obj.as_tree().unwrap().iter() {
      file_hash(repo, size, hasher, &e.to_object(repo).unwrap());
    }
  }
}

pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

pub struct Odb {
  repodir: PathBuf,
  // TODO These are always 40 byte ASCII, should use a non-heap String type:
  packs: RefCell<Vec<String>>,
  midxs: RefCell<Vec<String>>,
}

impl Odb {
  pub fn open(repodir: PathBuf) -> Result<Self, Fail> {
    // Just see if the given directory appears to be a git repository
    std::fs::File::open(&repodir)?;
    std::fs::File::open(&repodir.join("objects"))?;
    std::fs::File::open(&repodir.join("refs"))?;
    let ret = Self {
      repodir,
      packs: Default::default(),
      midxs: Default::default(),
    };
    ret.init()?;
    Ok(ret)
  }
  // Initialize caches, can call this anytime, if repo content has changed on disk, this will
  // of course make Odb return different results.
  pub fn init(&self) -> Result<(), Fail> {
    self.reset()?;
    self.discover_packs()?;
    self.discover_midxs()?;
    Ok(())
  }
  // Internal use only: Used by init(). Must be followed by reading again.
  fn reset(&self) -> Result<(), Fail> {
    Ok(())
  }
  fn discover_packs(&self) -> Result<(), Fail> {
    for n in self.repodir.join("objects").join("pack").read_dir()? {
      let n = n?;
      if n.file_type()?.is_file() {
        match n.file_name().into_string() {
          Err(_) => (),
          Ok(n) => {
            if n.len() == 50 && n.starts_with("pack-") && n.ends_with(".pack") {
              let n = n[5..45].to_string();
              std::fs::File::open(self.repodir.join(format!("objects/pack/pack-{}.idx", n)))?;
              self.packs.borrow_mut().push(n);
            }
          }
        }
      }
    }
    Ok(())
  }
  // TODO maybe join file discovery code for pack and midx cases
  fn discover_midxs(&self) -> Result<(), Fail> {
    for n in self.repodir.join("objects").join("pack").read_dir()? {
      let n = n?;
      if n.file_type()?.is_file() {
        match n.file_name().into_string() {
          Err(_) => (),
          Ok(n) => {
            if n.len() == 50 && n.starts_with("midx-") && n.ends_with(".midx") {
              let n = n[5..45].to_string();
              self.midxs.borrow_mut().push(n);
            }
          }
        }
      }
    }
    Ok(())
  }
  pub fn for_each_oid<F: Fn(&Oid)>(&self, func: F) -> Result<F, Fail> {
    let func = self.for_each_oid_in_packs(func)?;
    Ok(func)
  }
  /*
  Discover all files without cycles.
  I want to loop over all files because I don't want to miss something.
  But I should not discover files twice.
  Therefore, I should not recurse into trees.
  Just use any .bup and .bupl
  */
  pub fn for_each_file<F: Fn(&File) -> Result<(), Fail>>(&self, func: F) -> Result<F, Fail> {
    self.for_each_obj(|obj| {
      // TODO should Obj be an enum, or contain an enum?
      if let Some(tree) = obj.to_tree()? {
        // TODO out of interest, track the origin of an entry, is each packfile self-contained?
        // It's most likely not guaranteed to be self contained, but would be interesting for opt.
        for e in tree.entries {
          if e.name.ends_with(".bup") {
            eprintln!(".bup entry {}", e.name);
            if e.mode == 40000 {
              /*
              TODO make sure that I never hold on to the file, because the func
              probably wants to access the contents as well which in turn
              must read from the Odb.
              Construct a File here, which will read the actual chunks only on iter consume.
              */
              File::new(self, e.oid);
            }
            else if e.mode == 100644 {
            }
            else {
              panic!("unknown mode: {}  {}", e.mode, e.name);
            }
          }
          else if e.name.ends_with(".bupm") {
            unimplemented!()
          }
          else if e.name.ends_with(".bupl") {
            // can probably be a folder, or a file, if the original ends with .bup or .bupm
            // try that on bup
            unimplemented!()
          }
          else {
            // this should be a directory.
            // do not traverse, if there are files, we will discover them anyway.
          }
        }
      }
      Ok(())
    })?;
    Ok(func)
  }
  fn for_each_oid_in_packs<F: Fn(&Oid)>(&self, func: F) -> Result<F, Fail> {
    let mut func = func;
    for pack in self.packs.borrow().iter() {
      let pack = Pack::open(&self.repodir.join("objects").join("pack"), pack)?;
      func = pack.for_each_oid(func)?;
    }
    Ok(func)
  }
  pub fn for_each_obj<F: Fn(&Obj) -> Result<(), Fail>>(&self, func: F) -> Result<F, Fail> {
    let mut func = func;
    for pack in self.packs.borrow().iter() {
      // TODO keep cache of open Pack
      let pack = Pack::open(&self.repodir.join("objects").join("pack"), pack)?;
      func = pack.for_each_obj(func)?;
    }
    Ok(func)
  }
  pub fn print_all_file_sha1(&self) -> Result<(), Fail> {
    self.for_each_file(|file| {
      eprintln!("TODO do something with the file {:?}", file);
      Ok(())
    })?;
    Ok(())
  }
  pub fn find_obj_by_oid(&self, oid: &Oid) -> Result<Option<Obj>, Fail> {
    eprintln!("Odb find_obj_by_oid {}", oid);
    // TODO keep cache of open Pack
    for packfn in self.packs.borrow().iter() {
      let pack = Pack::open(&self.repodir.join("objects").join("pack"), packfn).map_err(|e|fail!("could not open pack {:?}  {:?}", packfn, e))?;
      if let Some(obj) = pack.find_obj_by_oid(oid)? { return Ok(Some(obj)) }
    }
    Ok(None)
  }
  /*
  Object in a pack:
  sz = bits 0..4
  bits 4..7 indicate the type: 1 commit, 2 tree, 3 blob, 4 tag
  The size is a concatenation of bits 0..4 ~ 8..16 ~ 16..32
  so the first shift is only 4 bits, after that always 7 more.
  Then inflate.

  Loose object:
  Inflate.
  Find the first ' ', up to there is a string indicating the type.
  Find a \0 treat it as string rep of the size.
  The remainder is payload.
  */
}

impl std::fmt::Debug for Odb {
  fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
    write!(fmt, "Odb\n")?;
    write!(fmt, "Packs:\n")?;
    for x in self.packs.borrow().iter() {
      write!(fmt, "  {}\n", x)?;
    }
    write!(fmt, "Midxs:\n")?;
    for x in self.midxs.borrow().iter() {
      write!(fmt, "  {}\n", x)?;
    }
    Ok(())
  }
}

#[test] fn open_odb() {
  let odb = Odb::open("test/data/bup01".into()).unwrap();
  odb.for_each_oid(|oid| eprintln!("found oid {:?}", oid)).unwrap();
  eprintln!("{:?}", odb);
  assert!(false);
}

#[test] fn find_wrong_oid_returns_empty() {
  let odb = Odb::open("/Users/dwerder/test_bup".into()).unwrap();
  eprintln!("{:?}", odb);
  let obj = odb.find_obj_by_oid(&Oid::from_hex("0000000000000000000000000000000000011111").unwrap()).unwrap();
  eprintln!("{:?}", obj);
  assert!(obj.is_none());
}

#[test] fn find_obj_by_oid() {
  let odb = Odb::open("/Users/dwerder/test_bup".into()).unwrap();
  eprintln!("{:?}", odb);
  let oid = Oid::from_hex("66524634ea09459bf8ef3bc396ffc3b8ecc6d045").unwrap();
  let obj = odb.find_obj_by_oid(&oid).unwrap().unwrap();
  eprintln!("{:?}", obj);
  assert!(obj.oid == oid);
}

/**
An opened git pack, it keeps open file descriptors.
*/
pub struct Pack {
  fpack: RefCell<std::fs::File>,
  fidx: RefCell<std::fs::File>,
  #[allow(unused)] version: u32,
  #[allow(unused)] fanout: [u32; 256],
  nsha1: u32,
  fpacklen: u32,
  //mmap_idx: &'static [u8],
  //mmap: &'static [u8],
}

impl Pack {
  pub fn open(path: &Path, name: &str) -> Result<Self, Fail> {
    let mut fpack = std::fs::File::open(path.join(format!("pack-{}.pack", name)))?;
    let mut fidx = std::fs::File::open(path.join(format!("pack-{}.idx", name)))?;
    let mut header = [0; 8];
    fidx.read(&mut header)?;
    let version;
    if &header[0..4] == b"\xfftOc" {
      version = u32::from_be_bytes(*unsafe{transmute::<_, &[u8;4]>(&header[4])});
    }
    else {
      //version = 1;
      unimplemented!();
    }
    let mut fanout = [0u32; 256];
    seek(&mut fidx, SeekFrom::Start(8))?;
    let b2 = unsafe{std::slice::from_raw_parts_mut(fanout.as_mut_ptr() as *mut u8, 256 * std::mem::size_of::<u32>())};
    if fidx.read(b2)? != b2.len() {
      return Err(fail!("unexpected eof"))
    }
    {
      // Is this allowed? Don't I have two mut ref to the same data here?
      let a = unsafe{std::slice::from_raw_parts_mut(fanout.as_mut_ptr() as *mut [u8;4], 256)};
      for x in a {
        let g = *x;
        *x = u32::from_be_bytes(g).to_ne_bytes();
      }
    }

    #[allow(unused)]
    let fidxlen = seek(&mut fidx, SeekFrom::End(0))? as u32;
    let fpacklen = seek(&mut fpack, SeekFrom::End(0))? as u32;
    /*
    let fpack_for_mmap = std::fs::File::open(path.join(format!("pack-{}.pack", name)))?;
    let fidx_for_mmap = std::fs::File::open(path.join(format!("pack-{}.idx", name)))?;
    let raw_fd_idx = std::os::unix::io::AsRawFd::as_raw_fd(&fidx_for_mmap);
    let raw_fd_pack = std::os::unix::io::AsRawFd::as_raw_fd(&fpack_for_mmap);
    let mmap_ptr;
    let mmap_idx_ptr;
    unsafe {
      mmap_ptr = libc::mmap(std::ptr::null_mut(), fpacklen as usize, libc::PROT_READ, libc::MAP_SHARED, raw_fd_pack, 0);
      if mmap_ptr == libc::MAP_FAILED {
        return Err(fail!("mmap failed"))
      }
      mmap_idx_ptr = libc::mmap(std::ptr::null_mut(), fidxlen as usize, libc::PROT_READ, libc::MAP_SHARED, raw_fd_idx, 0);
      if mmap_idx_ptr == libc::MAP_FAILED {
        return Err(fail!("mmap idx failed"))
      }
    }
    */

    let nsha1 = fanout[255];
    //eprintln!("nsha1: {}", nsha1);
    Ok(Self {
      fpack: RefCell::new(fpack),
      fidx: RefCell::new(fidx),
      version,
      fanout,
      nsha1,
      fpacklen,
      //mmap_idx: unsafe{std::slice::from_raw_parts(mmap_idx_ptr as *const u8, fidxlen as usize)},
      //mmap: unsafe{std::slice::from_raw_parts(mmap_ptr as *const u8, fpacklen as usize)},
    })
  }
  pub fn fidx_mut(&self) -> RefMut<dyn ReadSeek> {
    self.fidx.borrow_mut()
  }
  pub fn fpack_mut(&self) -> RefMut<dyn ReadSeek> {
    self.fpack.borrow_mut()
  }
  pub fn for_each_oid<F: Fn(&Oid)>(&self, func: F) -> Result<F, Fail> {
    let mut f = self.fidx_mut();
    let f = f.deref_mut();
    let mut ni = self.nsha1;
    if true {
      unimplemented!("must not hold the borrow");
    }
    seek(f, SeekFrom::Start(8 + 256 * 4))?;
    const N: u32 = 1;
    let mut buf = [0u8; (N * 20) as usize];
    loop {
      //eprintln!("ni: {}", ni);
      if ni == 0 {
        //eprintln!("break");
        break;
      }
      let b2 = &mut buf[0..(ni.min(N)*20) as usize];
      //eprintln!("try to read {}", b2.len());
      if f.read(b2)? != b2.len() {
        return Err(fail!("unexpected eof"))
      }
      for oid in unsafe{std::slice::from_raw_parts(b2.as_ptr() as *const [u8;20], b2.len()/20)} {
        func(&Oid::new(*oid));
      }
      ni -= N;
    }
    Ok(func)
  }
  pub fn off_to_sha1(&self) -> usize { 8 + 256 * 4 }
  pub fn off_to_offset(&self) -> usize { (8 + 256 * 4 + self.nsha1 as usize * 24) }
  pub fn for_each_oid_off<F: Fn(&Oid, u32) -> Result<(), Fail>>(&self, func: F) -> Result<F, Fail> {
    let mut f = self.fidx_mut();
    let mut ncur = 0;
    let nend = self.nsha1;
    let foff1 = self.off_to_sha1() as u64;
    let foff2 = self.off_to_offset() as u64;
    /*
    TODO
    ✘ The offsets in the offset list are ordered by the sha1, so they are not ordered anyway!
    To get the compressed object size, try to read also the offset of the next if a next exists.
    Make the buffers large enough to hold one more item.
    But if at the end of the file, read only the available and compute the last length manually.
    */
    // max number of items to actually read, excluding the additional last item
    const NMAXBATCH: u32 = 2;
    const NBUF: usize = NMAXBATCH as usize + 1;
    let mut buf1 = [[0u8;20]; NBUF];
    let mut buf2 = [0u32; NBUF];
    loop {
      if ncur == nend {
        break;
      }
      // number of loopable items to read, subject to availability and limit
      let i = (nend - ncur).min(NMAXBATCH);
      // read one more if available
      let one_extra_avail = if ncur + i < nend { true } else { false };

      let b1 = &mut buf1[..(1+i) as usize];
      let b2 = &mut buf2[..(1+i) as usize];
      //eprintln!("try to read {} items (including the extra)", b1.len());
      {
        let ii = if one_extra_avail { 1 + i } else { i };
        // TODO is this really ok? b1 and b11 are mut ref to the same data!
        let b11 = unsafe{from_raw_parts_mut(b1.as_mut_ptr() as *mut u8, ii as usize * 20)};
        let b22 = unsafe{from_raw_parts_mut(b2.as_mut_ptr() as *mut u8, ii as usize *  4)};
        seek(f.deref_mut(), SeekFrom::Start(foff1 + ncur as u64 * 20))?;
        if f.read(b11)? != b11.len() { return Err(fail!("unexpected eof")) }
        seek(f.deref_mut(), SeekFrom::Start(foff2 + ncur as u64 *  4))?;
        if f.read(b22)? != b22.len() { return Err(fail!("unexpected eof")) }

        /*
        This uses memory mapped data:
        b1[..ii as usize].copy_from_slice(unsafe {
          from_raw_parts(
            self.mmap_idx.as_ptr().add(foff1 as usize + ncur as usize * 20) as *const [u8;20],
            ii as usize,
          )
        });
        b2[..ii as usize].copy_from_slice(unsafe {
          from_raw_parts(
            self.mmap_idx.as_ptr().add(foff2 as usize + ncur as usize *  4) as *const u32,
            ii as usize,
          )
        });
        */
      }
      for x in b2.iter_mut() {
        *x = u32::from_be_bytes(x.to_ne_bytes());
      }
      if !one_extra_avail {
        b2[b2.len()-1] = self.fpacklen - 20;
      }
      {
        let i = i as usize;
        for ((sha1, o1), _o2) in b1[..i].iter().zip(&b2[..i]).zip(&b2[1..]) {
          func(&Oid::new(*sha1), *o1)?;
        }
      }
      ncur += i;
    }
    Ok(func)
  }
  pub fn for_each_obj<F: Fn(&Obj) -> Result<(), Fail>>(&self, func: F) -> Result<F, Fail> {
    // TODO
    // can not use both files simultaneously.
    // use idx to get the hash and offsets.
    // use the pack to extract the data.
    #[allow(unused)] struct FF<F: Fn(&Obj)> {
      f: F,
    }
    self.for_each_oid_off(|oid, off| {
      func(&self.obj_from_off(off, oid.clone()).unwrap())?;
      Ok(())
    })?;
    Ok(func)
  }
  pub fn obj_from_off(&self, off: u32, oid: Oid) -> Result<Obj, Fail> {
    //eprintln!("off: {}", off);
    let mut f = self.fpack_mut();
    let mut f = f.deref_mut();

    seek(f, SeekFrom::Start(off as u64))?;
    let mut buf = [0; 4096];
    let n = f.read(&mut buf)?;
    if n < 1 { return Err(fail!("unexpected eof")) }
    let buf = buf;

    /*
    mmap approach:
    let buf = &self.mmap[off as usize..off as usize + 64];
    */
    let tyu8 = (buf[0] >> 4) & 0x7;
    let ty = if tyu8 == 1 { ObjTy::Commit }
    else if tyu8 == 2 { ObjTy::Tree }
    else if tyu8 == 3 { ObjTy::Blob }
    else if tyu8 == 4 { ObjTy::Tag }
    else { return Err(fail!("unexpected type tag: {}", tyu8)) };
    let (size, used) = size_from_varheader(&buf);
    if size > 2 * 1024 * 1024 { return Err(fail!("above 2 MB limit: {}", size)) }
    /*
    It is difficult to determine the length of the compressed data.
    The size as given in the object header is the uncompressed len.
    For the moment, seek to begin of content and give &mut Read to flate2
    and rely on flate2 to stop at some point.
    */
    /*
    if false {
      let mut content = vec![0u8; size as usize + 2048];
      // TODO optimize for small content, maybe one read is already enough.
      eprintln!("seek to content  {}  {}  {}", off, used, size);
      seek(f, SeekFrom::Start(off as u64 + used as u64))?;
      let nread = f.read(&mut content)?;
      // if nread != content.len() { return Err(fail!("unexpected eof")) }
      //eprintln!("content: {:?}", &content);
      //eprintln!("content: {}", hex::encode(&content));
      // https://stackoverflow.com/questions/9050260/what-does-a-zlib-header-look-like
      // Starts most often with 120, 156
      //eprintln!("content: {}", String::from_utf8_lossy(&content));
    }
    */
    /*
    use compression::prelude::*;
    content.iter().cloned().decode(&mut Deflater::new()).take(8).collect::<Result<Vec<_>, _>>().unwrap();
    */
    /*
    use std::ops::DerefMut;
    if false {
      use std::io::{Read, Cursor};
      use libflate::zlib::Decoder;
      let mut dec = Decoder::new(f.deref_mut()).unwrap();
      let mut buf = vec![];
      dec.read_to_end(&mut buf).unwrap();
      eprintln!("dec: {:?}", buf);
      eprintln!("dec: {}", String::from_utf8_lossy(&buf));
    }
    */
    let content;
    {
      use flate2::read::ZlibDecoder;
      // TODO use custom Read type which can reuse the read data from above, and read more if needed
      seek(f.deref_mut(), SeekFrom::Start(off as u64 + used as u64))?;
      let mut dec = ZlibDecoder::new(f.deref_mut());
      //let mut dec = ZlibDecoder::new(Cursor::new(&content));
      //let mut dec = ZlibDecoder::new(Cursor::new(&self.mmap[(off as usize + used as usize) .. self.fpacklen as usize]));
      let mut buf = vec![];
      dec.read_to_end(&mut buf).unwrap();
      //eprintln!("dec: {}   {}", buf.len(), String::from_utf8_lossy(&buf));
      content = buf;
    }
    Ok(Obj {
      oid,
      ty,
      size,
      content,
    })
  }
  pub fn find_ix_by_oid(&self, oid: &Oid) -> Result<Option<u32>, Fail> {
    let mut j1 = 0;
    let mut j2 = self.nsha1 as usize;
    // TODO unified interface to access data, indep of mmap or io::Read

    // Read via io
    let mut fidx = self.fidx_mut();
    let fidx = fidx.deref_mut();
    let mut buf = vec![0; self.nsha1 as usize * 20];
    seek(fidx, SeekFrom::Start(self.off_to_sha1() as u64))?;
    let n = fidx.read(&mut buf)?;
    if n != buf.len() { return Err(fail!("unepxected eof, read n: {}", n)) }
    let buf = buf;
    let a = unsafe{from_raw_parts(buf.as_ptr() as *const [u8;20], self.nsha1 as usize)};

    /*
    // Read via mmap
    let a = unsafe{from_raw_parts(&self.mmap_idx[self.off_to_sha1()] as *const _ as *const [u8;20], self.nsha1 as usize)};
    */

    let mut n = 0;
    loop {
      let jm = (j1 + j2) / 2;
      if oid.sha1 < a[jm] {
        j2 = jm;
      }
      else if oid.sha1 > a[jm] {
        j1 = 1 + jm;
      }
      else {
        return Ok(Some(jm as u32))
      }
      if j1 == j2 {
        return Ok(None)
      }
      n += 1;
      if n > 100 {
        panic!("binary search too deep oid: {}  {:?}  {}  {}", oid, self, j1, j2)
      }
    }
  }
  pub fn find_obj_by_oid(&self, oid: &Oid) -> Result<Option<Obj>, Fail> {
    match self.find_ix_by_oid(oid)? {
      None => Ok(None),
      Some(ix) => {
        Ok(Some(self.obj_from_off(self.get_offset(ix), oid.clone())?))
      }
    }
  }
  pub fn get_offset(&self, ix: u32) -> u32 {
    // TODO unified interface to access data, indep of mmap or io::Read
    let mut fidx = self.fidx_mut();
    let fidx = fidx.deref_mut();
    let mut buf = [0u8; 4];
    seek(fidx, SeekFrom::Start(self.off_to_offset() as u64 + ix as u64 * 4)).unwrap();
    if fidx.read(&mut buf).unwrap() != 4 { panic!("unexpected eof") }
    u32::from_be_bytes(buf)

    // read via mmap
    /*
    let a = unsafe{from_raw_parts(&self.mmap_idx[self.off_to_offset()] as *const _ as *const [u8;4], self.nsha1 as usize)};
    u32::from_be_bytes(a[ix as usize])
    */
  }
}

impl std::fmt::Debug for Pack {
  fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
    write!(fmt, "Pack {:?}", self.fpack)
  }
}

/// Return the decoded u32 and the number of used bytes.
fn size_from_varheader(buf: &[u8]) -> (u32, usize) {
  let mut size = (buf[0] & 0xf) as u32;
  let mut i = 0;
  loop {
    if buf[i] & 0x80 == 0 { return (size, 1 + i) }
    if i == 4 { panic!("Malformed var length encoded size: {}", hex::encode(&buf[..5])) }
    i += 1;
    if i == 1 { size |= ((buf[i] & 0x7f) as u32) << 4; }
    else { size |= ((buf[i] & 0x7f) as u32) << (i * 7 - 3); }
  }
}

#[test] fn pack_open() {
  // The pack should have a commit 56c2dad
  let pack = Pack::open(Path::new("test/data"), "7087f2c06aae6abf9adefbe57a117733d2056e06").unwrap();
  pack.for_each_oid(|oid|eprintln!("got {}", oid.to_string())).unwrap();
  //pack.for_each_obj(|obj|eprintln!("got {}", obj)).unwrap();
  assert!(false);
}

#[derive(Clone, Debug, PartialEq)]
pub struct Oid {
  sha1: [u8; 20],
}

impl Oid {
  pub fn new(sha1: [u8; 20]) -> Self {
    Self {
      sha1,
    }
  }
  pub fn from_hex(hex: &str) -> Result<Self, Fail> {
    let sl = hex::decode(hex).map_err(|e|faildb!(e))?;
    if sl.len() != 20 { return Err(fail!("unexpected length != 20: {}", sl.len())) }
    let mut buf = [0;20];
    for i in 0..buf.len() { buf[i] = sl[i]; }
    Ok(Oid::new(buf))
  }
  pub fn to_string(&self) -> String {
    hex::encode(self.sha1)
  }
}

impl std::fmt::Display for Oid {
  fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
    write!(fmt, "{}", hex::encode(self.sha1))
  }
}

#[derive(Debug)]
pub struct Obj {
  oid: Oid,
  ty: ObjTy,
  size: u32,
  content: Vec<u8>,
}

impl Obj {
  pub fn oid(&self) -> &Oid { &self.oid }
  pub fn is_tree(&self) -> bool { match self.ty { ObjTy::Tree => true, _ => false } }
  pub fn is_commit(&self) -> bool { match self.ty { ObjTy::Commit => true, _ => false } }
  pub fn to_tree(&self) -> Result<Option<Tree>, Fail> {
    if !self.is_tree() { return Ok(None) }
    let mut cur = &self.content[..];
    let mut entries = vec![];
    //let rx = regex::Regex::new(r"(\d\d\d\d\d) ()\x00");
    loop {
      let entry_len;
      match cur.iter().position(|x| *x == 0) {
        Some(posnull) => {
          // s1 has mode and name
          // s2 has sha1
          let (s1, s2) = cur.split_at(posnull);
          if s2.len() < 21 {
            let s = String::from_utf8_lossy(&self.content);
            eprintln!("PROBLEM?? {:?} {:?}", self.content, s);
          }
          match s1.iter().position(|x|*x==b' ') {
            Some(posspace) => {
              let (s3, s4) = s1.split_at(posspace);
              entry_len = s2[21..].as_ptr() as usize;
              let x = TreeEntry {
                oid: Oid::new(*unsafe{transmute::<_, &[u8; 20]>(&s2[1..21][0])}),
                mode: String::from_utf8(s3.into()).map_err(|e|faildb!(e))?.parse().map_err(|e|faildb!(e))?,
                name: String::from_utf8(s4[1..].into()).map_err(|e|faildb!(e))?,
              };
              if x.name.ends_with(".bup") {
                eprintln!("{:?}", x);
              }
              entries.push(x);
            }
            _ => return Err(fail!("can not find space supposed to be after mode"))
          }
        }
        _ => return Err(fail!("can not find the null"))
      };
      let new = entry_len - cur.as_ptr() as usize;
      cur = &cur[new..];
      if cur.len() == 0 {
        break
      }
    }
    Ok(Some(Tree {
      entries,
    }))
  }
}

impl std::fmt::Display for Obj {
  fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
    let _s = String::from_utf8_lossy(&self.content);
    write!(fmt, "Obj ty: {:?}  size: {}\nEND Obj", self.ty, self.size)
  }
}

#[derive(Debug)]
pub enum ObjTy {
  Blob,
  Tree,
  Commit,
  Tag,
}

impl ObjTy {
  pub fn as_u8(&self) -> u8 {
    use ObjTy::*;
    match self {
      Commit => 1,
      Tree => 2,
      Blob => 3,
      Tag => 4,
    }
  }
}

pub struct TreeEntry {
  mode: u32,
  oid: Oid,
  name: String,
}

impl std::fmt::Debug for TreeEntry {
  fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
    write!(fmt, "TreeEntry  {:6}  {}  {}", self.mode, self.oid, self.name)
  }
}

pub struct Tree {
  entries: Vec<TreeEntry>,
}

impl Tree {
  pub fn for_each_file<F: Fn(())>(&self, func: F) -> Result<F, Fail> {
    Ok(func)
  }
  pub fn entries(&self) -> &[TreeEntry] {
    &self.entries
  }
}

impl std::fmt::Debug for Tree {
  fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
    write!(fmt, "Tree")?;
    for e in &self.entries {
      write!(fmt, "  {:?}", e)?;
    }
    Ok(())
  }
}

pub struct FileContentIter<'a> {
  _odb: &'a Odb,
  _oid_beg: Oid,
  _stack: Vec<Oid>,
}

impl<'a> FileContentIter<'a> {
  fn _recurse(&mut self) -> Result<(), Fail> {
    // need a stack for the trees and the current position at each level
    // read oid_next from obj
    // if tree,
    unimplemented!()
  }
}

impl<'a> Iterator for FileContentIter<'a> {
  type Item = Vec<u8>;
  fn next(&mut self) -> Option<Self::Item> {
    unimplemented!()
  }
}

#[derive(Debug)]
pub struct File<'a> {
  #[allow(unused)] odb: &'a Odb,
  #[allow(unused)] oid_beg: Oid,
}

impl<'a> File<'a> {
  pub fn new(odb: &'a Odb, oid_beg: Oid) -> Self {
    File {
      odb,
      oid_beg,
    }
  }
}

pub struct Midx<T: ReadSeek> {
  #[allow(unused)] version: u32,
  bits: u32,
  fanout: Vec<u8>,
  off_sha1: usize,
  // TODO use better name as soon as I know what 'which' means
  off_which: usize,
  read: Box<T>,
}

impl<T: ReadSeek> Midx<T> {
  pub fn open(mut read: Box<T>) -> Result<Self, Fail> {
    let flen = seek(&mut read, SeekFrom::End(0))? as usize;
    let mut buf = [0u8; 512];
    seek(&mut read, SeekFrom::Start(0))?;
    let n = read.read(&mut buf)?;
    if n < 12 { return Err(fail!("too short for being a midx header")) }
    if &buf[0..4] != b"MIDX" {
      return Err(fail!("wrong magic number"))
    }
    let version = u32::from_be_bytes(*unsafe{transmute::<_, &[u8;4]>(&buf[4])});
    const MIDX_VERSION: u32 = 4;
    if version != MIDX_VERSION { return Err(fail!("expect midx version {} but got {}", MIDX_VERSION, version)) }
    let bits = u32::from_be_bytes(*unsafe{transmute::<_, &[u8;4]>(&buf[8])});
    if bits > 22 { return Err(fail!("bits too large: {}", bits)) }
    let entries = 1 << bits;
    if entries > 4 * 1024 * 1024 {
      return Err(fail!("too many entries: {}", entries))
    }
    let mut fanout;
    {
      fanout = vec![0; 4 * entries];
      seek(&mut read, SeekFrom::Start(12))?;
      if read.read(&mut fanout)? != fanout.len() {
        return Err(fail!("could not read exact number of fanout bytes"))
      }
    }
    let nsha1 = u32::from_be_bytes(*unsafe{transmute::<_, &[u8;4]>(&fanout[fanout.len()-4])}) as usize;
    let flen_expected = 12 + 4 * entries + (20+4) * nsha1;
    if flen_expected > flen { return Err(fail!("Missing bytes: {}", flen_expected - flen)) }
    let off_fanout = 12;
    let off_sha1 = off_fanout + 4 * entries;
    let off_which = off_sha1 + 20 * nsha1;
    {
      // is each 'which' maybe a u32 to indicate the offset into the packfile?
      let s = off_which + 4 * nsha1;
      eprintln!("flen: {}  off_which: {}  nsha1: {}  s: {}", flen, off_which, nsha1, s);
      seek(&mut read, SeekFrom::Start(s as u64))?;
      let mut buf = vec![0u8; flen - s];
      if read.read(&mut buf)? != buf.len() {
        return Err(fail!("read returned unexpected len"))
      }
      let idxfilenames = String::from_utf8(buf.clone()).map_err(|e|faildb!(e))?;
      let a = idxfilenames.split("\0");
      for x in a {
        eprintln!("{:?}", x);
      }
    }
    let ret = Midx {
      version,
      bits,
      fanout,
      off_sha1,
      off_which,
      read: read,
    };
    Ok(ret)
  }
  pub fn entries(&self) -> u32 { 1 << self.bits }
  pub fn fanout(&self) -> &[u8] { &self.fanout }
  pub fn sha1_offs(&self) -> u32 { 12 + 4 * self.entries() }
  pub fn nsha1(&self) -> u32 { self.fanget(self.entries() - 1) }
  fn fanget(&self, i: u32) -> u32 {
    let n = 4 * i as usize;
    u32::from_be_bytes(*unsafe{transmute::<_, &[u8;4]>(&self.fanout[n])})
  }
  pub fn fan(&mut self, i: u32) -> u32 {
    let n = 12 + 4 * i as u64;
    seek(&mut self.read, SeekFrom::Start(n)).unwrap();
    let mut ret = [0; 4];
    if self.read.read(&mut ret).unwrap() != ret.len() {
      panic!("read failed")
    }
    u32::from_be_bytes(ret)
  }
  pub fn sha1(&mut self, i: u32) -> [u8; 20] {
    let n = self.off_sha1 as u64 + 20 * i as u64;
    seek(&mut self.read, SeekFrom::Start(n)).unwrap();
    let mut ret = [0u8; 20];
    if self.read.read(&mut ret).unwrap() != ret.len() {
      panic!("read failed")
    }
    ret
  }
  pub fn which(&mut self, i: u32) -> u32 {
    let n = self.off_which as u64 + 4 * i as u64;
    seek(&mut self.read, SeekFrom::Start(n)).unwrap();
    let mut ret = [0; 4];
    if self.read.read(&mut ret).unwrap() != ret.len() {
      panic!("read failed")
    }
    u32::from_be_bytes(ret)
  }
  pub fn contains_sha1(&mut self, sha1: &[u8; 20]) -> bool {
    let mut i1 = 0;
    let mut i2 = self.nsha1();
    loop {
      eprintln!("i1: {}  i2: {}", i1, i2);
      let mid = (i1 + i2) / 2;
      let g = self.sha1(mid);
      if sha1 < &g {
        if i2 == mid {
          return false;
        }
        i2 = mid;
      }
      else if sha1 > &g {
        if i1 == 1 + mid {
          return false;
        }
        i1 = 1 + mid;
      }
      else {
        return true;
      }
    }
  }
}

#[test] fn midx_read() {
  let f1 = "/Users/dwerder/vmwr/midx-07fee57637bd637c7a4ae85879ded24731ad2f15.midx";
  let f2 = "/Users/dwerder/test_bup/objects/pack/midx-0d58e72e68f0273f6f8d7f523bbc529e7cddb33f.midx";
  let f3 = "/Users/dwerder/bup2/objects/pack/midx-3dc965112c6d56d6f5ae9255d464226216ef9b94.midx";
  let fpath = std::path::Path::new(f3);
  let fmidx = std::fs::File::open(fpath).unwrap();
  let mut m = Midx::open(Box::new(fmidx)).unwrap();
  eprintln!("Entries: {}", m.entries());
  if false {
    for i in 0..m.entries() {
      eprintln!("{}", m.fan(i));
    }
  }
  if false {
    for i in 0..m.nsha1() {
      eprintln!("{}  {}", hex::encode(m.sha1(i)), m.which(i));
    }
  }
  assert!(false);
}
