#![crate_name = "ssdb"]
//[comment = "SSDB client"]
//#![crate_type = "dylib"]
//#![crate_type = "rlib"]
extern crate bufstream;

mod ssdb{
    use std::net::{TcpStream};
    use std::io::{BufRead,Read,Write,Result,Error,ErrorKind};
    use std::string::String;
    use std::convert::AsRef;
    use bufstream::BufStream;

    pub enum SSDBResult {
        Nil,
        Int(String,i64),
        Data(String,Vec<u8>)
    }

    pub struct Client<S:Read+Write>{
        ip:String,
        port:u16,
        stream:Option<BufStream<S>>
    }

    fn invalid_input(desc: &'static str, detail: &str) -> Error {
        Error::new(ErrorKind::InvalidInput, desc)
    }

    fn read_byte<S: Read+Write>(stream:&mut BufStream<S>) ->Result<u8>{
        let buf: &mut[u8;1]=&mut [0];
        let nread = try!(stream.read(buf));
        if nread < 1 {
            Err(invalid_input("Read failed!","Could not read enough bytes"))
        } else {
            Ok(buf[0])
        }
    }

    fn read_item<S: Read+Write>(stream: &mut BufStream<S>) -> Result<String> {
        let l = &mut String::new();
        stream.read_line(l);
        let len=match l.trim().as_ref(){
            ""=>{return Ok("".to_string())},
            _=>{l.trim().parse::<usize>().unwrap_or(0)}
        };
        let mut s = vec![];
        s.reserve(len);
        for _ in 0..len{
            s.push(try!(read_byte(stream)));
        }
        read_byte(stream);
        
        Ok(String::from_utf8(s).unwrap_or(String::new()))
    }
    
    pub fn read_status<S: Read+Write>(stream: &mut BufStream<S>)->Result<()> {
        match read_item(stream).unwrap_or("".to_owned()).as_ref(){
            "ok"=>Ok(()),
            resp@_=>Err(invalid_input("Operation failed!", resp.as_ref()))
        }
    }
    
    impl Client<TcpStream>{
        pub fn send(&mut self, data:Vec<&str>)->Result<Vec<String>>{
            let mut v=vec![];
            match self.stream{
                Some(ref mut stream) => {
                    for item in data.iter(){
                        writeln!(stream,"{}",item.len());
                        writeln!(stream,"{}",item);
                    }
                    writeln!(stream,"");
                    try!(stream.flush());
                    loop{
                        let s=try!(read_item(stream));
                        if s=="" { break; }
                        v.push(s); 
                    }
                },
                _ =>{panic!("Connect not open!");}
            }
            Ok(v)
        }
        
        pub fn send_req(&mut self, cmd:&str, param:Vec<&str>)->Vec<String>{
                        
            match self.stream{
                Some(ref mut stream) =>{
                    writeln!(stream, "{}", cmd.len());
                    writeln!(stream, "{}", cmd);
                },
                _ => {panic!("Connect not open!");}
            }
            self.send(param).unwrap_or(vec![])
        }

        pub fn parse(&mut self, cmd:&str, resp:Vec<String>)->SSDBResult{
            match cmd {
                "getbit" | "setbit" | "countbit" | "strlen" 
                    | "set" | "setx" | "setnx" | "zset" | "hset"
                    | "qpush" | "qpush_front" | "qpush_back"
                    | "del" | "zdel" | "hdel" | "hsize" | "zsize" | "qsize"
                    | "hclear" | "zclear" | "qclear"
                    | "multi_set" | "multi_del" | "multi_hset" | "multi_hdel" | "multi_zset" | "multi_zdel"
                    | "incr" | "decr" | "zincr" | "zdecr" | "hincr" | "hdecr"
                    | "zget" | "zrank" | "zrrank" | "zcount" | "zsum" | "zremrangebyrank" | "zremrangebyscore" 
                    =>SSDBResult::Int(resp[0].clone(), resp[1].parse::<i64>().unwrap_or(0)),
                "get"=>SSDBResult::Data(resp[0].clone(), resp[1].clone().into_bytes()),
                _ =>SSDBResult::Nil
            }
        }

        pub fn get(&mut self, key:&str)->SSDBResult{
            let resp=self.send_req("get",vec![key]);
            self.parse("get",resp)
        }

        pub fn set(&mut self, key:&str, value:&str)->SSDBResult{
            let resp=self.send_req("set",vec![key, value]);
            self.parse("set",resp)
        }

        pub fn new(ip:&str,port:i32)->Client<TcpStream>{
            Client{
                ip:ip.to_string(), 
                port:port as u16,
                stream:None,
            }
        }

        pub fn connect(&mut self){
            if !self.stream.is_none(){
                panic!("can not call connect() twice!");
            }

            let stream=TcpStream::connect((self.ip.as_ref(),self.port)).unwrap();
            self.stream=Some(BufStream::new(stream));
            println!("connected!");
        }

        pub fn close(&mut self){
            match self.stream {
                Some(ref mut stream) =>{ stream.flush(); },
                _ =>{}
            }
            self.stream=None; 
        }
        
    }
}

#[test]
fn it_works() {
    use std::thread;
    
    let mut handles=vec![];
    
    //Require ssdb daemon running
    for i in 0..3{
        let h=thread::spawn(move ||{
            let key="测试".to_owned()+i.to_string().as_ref();
            let mut foo=ssdb::Client::new("127.0.0.1",8888);
            foo.connect();
            foo.set(key.as_ref(),"foo");
            let resp=match foo.get(key.as_ref()){ssdb::SSDBResult::Data(_,resp)=>resp,_=>vec![]};
            assert!(String::from_utf8(resp).unwrap()=="foo");
            foo.close();
        });
        handles.push(h);
    }
    for h in handles{
        h.join(); 
    }
    //println!("{:?}",String::from_utf8(resp).unwrap());
    //foo.set("测试","foo");
}
