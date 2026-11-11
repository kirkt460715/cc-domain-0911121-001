import http from 'node:http';
import assert from 'node:assert/strict';
const record=(revision,value)=>({station:'station1',metric:'temperature',time:'2026-11-11T10:00:00Z',revision,value,unit:'C'});
const blocks=[
 ': 合成站点心跳\r\nretry: 50\r\n\r\n',
 'id: opaque-a\r\nevent: observation\r\ndata: '+JSON.stringify(record(1,20))+'\r\n\r\n',
 'id: opaque-b\nevent: observation\ndata: {"station":"station1",\ndata: "metric":"temperature","time":"2026-11-11T10:00:00Z","revision":2,"value":21,"unit":"C"}\n\n',
 'id: opaque-bad\ndata: {broken}\n\n',
 'id: opaque-c\ndata: '+JSON.stringify(record(3,22))+'\n\n'];
export function start(port=0){
 const seen=[];
 const server=http.createServer((req,res)=>{
  const url=new URL(req.url,'http://localhost');seen.push(req.headers['last-event-id']??null);
  if(url.pathname==='/requests'){res.setHeader('Content-Type','application/json');res.end(JSON.stringify(seen));return;}
  if(url.pathname!=='/events'){res.writeHead(404);res.end();return;}
  let start=0;const previous=req.headers['last-event-id'];
  if(previous){const found=blocks.findIndex(x=>x.includes('id: '+previous+'\n')||x.includes('id: '+previous+'\r'));if(found>=0)start=found;}
  let bytes=Buffer.from(blocks.slice(start).join(''));
  if(url.searchParams.has('conflict'))bytes=Buffer.from(blocks[1].replace(JSON.stringify(record(1,20)),JSON.stringify(record(1,99))));
  const cut=Number(url.searchParams.get('cut')||0);if(cut)bytes=bytes.subarray(0,cut);
  res.writeHead(200,{'Content-Type':'text/event-stream','Connection':'close'});
  let at=0;function pump(){while(at<bytes.length){const part=bytes.subarray(at,at+3);at+=part.length;if(!res.write(part)){res.once('drain',pump);return;}}res.end();}pump();
 });
 return new Promise(resolve=>server.listen(port,'127.0.0.1',()=>resolve({server,base:'http://127.0.0.1:'+server.address().port,seen})));
}
if(process.argv.includes('--check')){
 const {server,base,seen}=await start();try{
  const full=await(await fetch(base+'/events')).text();assert(full.includes('合成站点'));assert(full.includes('opaque-c'));
  const truncated=await(await fetch(base+'/events?cut=4')).arrayBuffer();assert.equal(truncated.byteLength,4);
  const resumed=await(await fetch(base+'/events',{headers:{'Last-Event-ID':'opaque-b'}})).text();assert(resumed.startsWith('id: opaque-b'));assert.equal(seen.at(-1),'opaque-b');
  assert((await(await fetch(base+'/events?conflict=1')).text()).includes('99'));
  console.log('事件流来源：切块、截断、重发、同号异文检查通过');
 }finally{server.closeAllConnections();server.close();}
}else{const {base}=await start(Number(process.env.PORT||8787));console.log('本地事件来源 '+base+'/events');}
