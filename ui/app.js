'use strict';
const $ = id => document.getElementById(id);
const token = document.querySelector('meta[name="client-token"]').content;
let connected = false, busy = false, uploading = false, cursor = 0;
const fileRows = new Map();
const terminal = new Set(['completed', 'failed', 'cancelled', 'expired']);
const labels = {preparing:'Chuẩn bị',waiting_accept:'Chờ chấp nhận',sending:'Đang gửi',receiving:'Đang nhận',verifying:'Chờ kiểm tra',completed:'Hoàn tất',failed:'Lỗi',cancelled:'Đã hủy',expired:'Hết hạn'};
function state(value) {
  connected = value;
  $('connection').textContent = value ? 'Đã kết nối' : 'Chưa kết nối';
  for (const id of ['recipient','text','send-chat','file','send-file','logout']) $(id).disabled = !value;
  $('login-button').disabled = value || busy;
  $('send-file').disabled = !value || uploading;
}
function error(e) { $('error').textContent = e.message || String(e); }
async function api(path, body) {
  const response = await fetch(path, {method:body === undefined ? 'GET':'POST',headers:{'X-Client-Token':token,...(body===undefined?{}:{'Content-Type':'application/json'})},body:body===undefined?undefined:JSON.stringify(body)});
  const value = await response.json();
  if (!response.ok) throw new Error(value.error || 'Thao tác thất bại');
  return value;
}
function message(text) {
  const item = document.createElement('li'); item.textContent = text;
  $('messages').append(item);
  while ($('messages').children.length > 500) $('messages').firstChild.remove();
  $('messages').scrollTop = $('messages').scrollHeight;
}
function fileUpdate(e) {
  const key = e.direction + ':' + e.transfer_id;
  let row = fileRows.get(key);
  if (!row) {
    if (fileRows.size >= 100) {
      const old = [...fileRows.entries()].find(([,v])=>v.done);
      if (old) { old[1].item.remove(); fileRows.delete(old[0]); }
    }
    const item=document.createElement('li'), text=document.createElement('span'), progress=document.createElement('progress'), button=document.createElement('button');
    button.textContent='Hủy';
    button.onclick=async()=>{try { await api('/api/cancel',{direction:e.direction,transfer_id:e.transfer_id}); } catch(err){error(err);} };
    item.append(text,progress,button); $('files').append(item);
    row={item,text,progress,button}; fileRows.set(key,row);
  }
  row.done=terminal.has(e.status);
  const bytes=e.direction==='send'?e.acknowledged_bytes:e.received_bytes;
  row.text.textContent=`${e.direction==='send'?'Gửi tới '+e.recipient:'Nhận từ '+e.sender}: ${e.name || e.transfer_id.slice(0,8)} — ${labels[e.status] || e.status} (${bytes}/${e.total_bytes ?? '?'} byte)`;
  if(e.cleanup_pending) row.text.textContent+=' — còn file tạm cần dọn';
  if(e.cancel_delivery==='failed') row.text.textContent+=' — chưa báo hủy được tới server';
  if(e.confirmation_delivery==='failed') row.text.textContent+=' — file đã lưu, gửi xác nhận thất bại';
  if(e.message) row.text.textContent+=' — '+e.message;
  row.progress.max=e.total_bytes || 1; row.progress.value=e.total_bytes===0&&e.status==='completed'?1:bytes;
  row.button.disabled=row.done||!connected;
}
function event(e) {
  if(e.type==='message') message(`${e.sender} → ${e.recipient==='*'?'tất cả':e.recipient}: ${e.text}`);
  else if(e.type==='error') error(e.message);
  else if(e.type==='file') fileUpdate(e);
  else if(e.type==='connection') state(e.connected);
  else if(e.type==='users') {
    const selected=$('recipient').value; $('recipient').replaceChildren();
    for(const name of ['*',...e.users]) {const option=document.createElement('option');option.value=name;option.textContent=name==='*'?'Tất cả':name;$('recipient').append(option);}
    if([...$('recipient').options].some(o=>o.value===selected)) $('recipient').value=selected;
  }
}
$('login').onsubmit=async e=>{e.preventDefault();busy=true;state(false);$('error').textContent='';try{await api('/api/login',{host:$('host').value,port:Number($('port').value),username:$('username').value,password:$('password').value});state(true);}catch(err){error(err);}finally{busy=false;$('password').value='';state(connected);}};
$('logout').onclick=async()=>{try{await api('/api/logout',{});state(false);}catch(err){error(err);}};
$('chat').onsubmit=async e=>{e.preventDefault();const text=$('text').value;if(new TextEncoder().encode(text).length>480){error('Tin nhắn vượt 480 byte');return;}try{await api('/api/messages',{recipient:$('recipient').value,text});message(`Bạn → ${$('recipient').value}: ${text} (đã yêu cầu gửi)`);$('text').value='';}catch(err){error(err);}};
$('upload').onsubmit=async e=>{e.preventDefault();const file=$('file').files[0],recipient=$('recipient').value;if(!file||recipient==='*'){error('Chọn file và một người nhận riêng');return;}if(file.size>16*1024*1024){error('File vượt 16 MiB');return;}uploading=true;$('send-file').disabled=true;$('upload-status').textContent='Đang chuyển file vào client local…';try{const response=await fetch(`/api/files?recipient=${encodeURIComponent(recipient)}&name=${encodeURIComponent(file.name)}`,{method:'POST',headers:{'X-Client-Token':token},body:file});const value=await response.json();if(!response.ok)throw new Error(value.error);$('upload-status').textContent='Đã nhận yêu cầu gửi; theo dõi kết quả bên dưới.';}catch(err){error(err);$('upload-status').textContent='Upload không hoàn tất';}finally{uploading=false;$('send-file').disabled=!connected;}};
async function poll(){try{const value=await api('/api/events?after='+cursor);if(value.reset)message('Một phần lịch sử đã hết thời gian lưu; đang hiển thị các sự kiện còn lại.');value.events.forEach(event);cursor=value.cursor;if(!busy)state(value.connected);}catch(err){state(false);error(err);}finally{setTimeout(poll,500);}}
state(false);poll();
