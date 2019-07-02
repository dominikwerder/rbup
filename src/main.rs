fn main() {
   env_logger::init();
  let app = clap::App::new("rbup").version("0.0.0")
  .author("Dominik Werder <dominik.werder@gmail.com>")
  .about("Complementary tools for everyday `bup` use.")
  .args_from_usage("
  --scan=[DIR]      'Scan all files'
  --explore         'Explore bup dir --bupdir'
  --check           'Compare a --scanlist with a --bupdir (not implemented)'
  --scanlist=[FILE] 'Scanlist file'
  --bupdir=[DIR]    'BUP_DIR'
  ");
  use std::path::Path;
  let matches = app.get_matches();
  if matches.is_present("scan") {
    let res = match rbup::scan::scan(std::path::Path::new(matches.value_of("scan").unwrap())) {
      Ok(_) => 0,
      Err(e) => {
        eprintln!("Error: {:?}", e);
        1
      }
    };
    std::process::exit(res);
  }
  else if matches.is_present("explore") {
    let repodir = Path::new(matches.value_of("bupdir").unwrap());
    let odb = rbup::rbup::Odb::open(repodir.into()).unwrap();
    odb.print_all_file_sha1().unwrap();
  }
  else if matches.is_present("check") {
    if true {
      unimplemented!("refactor into separate steps");
    }
    rbup::scan::check_sha1_in_bup(Path::new(matches.value_of("scanlist").unwrap()), Path::new(matches.value_of("bupdir").unwrap())).unwrap();
  }
}
