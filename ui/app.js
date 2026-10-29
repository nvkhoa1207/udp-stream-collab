'use strict';

const token = document.querySelector('meta[name="client-token"]').content;
const fileRows = new Map();
const terminalStatuses = new Set(['completed', 'failed', 'cancelled', 'expired']);
const statusLabels = {
  preparing: 'Preparing',
  waiting_accept: 'Waiting for receiver',
  sending: 'Sending',
  receiving: 'Receiving',
  verifying: 'Verifying SHA-256',
  completed: 'Completed',
  failed: 'Failed',
  cancelled: 'Cancelled',
  expired: 'Expired'
};

let connected = false;
let busy = false;
let uploading = false;
let sendingFile = false;
let cursor = 0;
let username = '';
let serverErrors = [];
const localErrors = new Map();

function getElement(id) {
  return document.getElementById(id);
}

// Dùng textContent để tên file và tin nhắn luôn được hiển thị như văn bản.
function createElement(tag, className, text = '') {
  const element = document.createElement(tag);
  element.className = className;
  element.textContent = text;
  return element;
}

function renderErrors() {
  const messages = [...serverErrors.map(error => error.message), ...localErrors.values()];
  getElement('error').textContent = messages.join(' ');
  getElement('dismiss-error').hidden = messages.length === 0;
}

function showError(error, scope = 'operation') {
  localErrors.set(scope, error.message || String(error));
  renderErrors();
}

function clearError(scope) {
  localErrors.delete(scope);
  renderErrors();
}

function updateConnection(value) {
  connected = value;
  // Khi mất phiên, danh sách cũ không còn là trạng thái online hiện tại.
  if (!connected) {
    updateUsers([]);
  }
  const status = getElement('connection');
  if (connected) {
    status.textContent = username ? 'Connected as ' + username : 'Connected';
    status.dataset.state = 'online';
  } else if (busy) {
    status.textContent = 'Connecting…';
    status.dataset.state = 'connecting';
  } else {
    status.textContent = 'Disconnected';
    status.dataset.state = 'offline';
  }

  const controls = ['recipient', 'text', 'send-chat', 'file', 'send-file', 'logout'];
  for (const id of controls) {
    getElement(id).disabled = !connected;
  }
  getElement('login-button').disabled = connected || busy;
  getElement('send-file').disabled = !connected || uploading || sendingFile;
  for (const button of getElement('user-list').querySelectorAll('button')) {
    button.disabled = !connected;
  }
  for (const row of fileRows.values()) {
    row.button.disabled = row.done || !connected;
  }
}

// Browser chỉ gọi HTTP bridge. Phần Python mới thực hiện gửi TCP/UDP.
async function api(path, body) {
  const options = {
    method: 'GET',
    headers: {'X-Client-Token': token}
  };
  if (body !== undefined) {
    options.method = 'POST';
    options.headers['Content-Type'] = 'application/json';
    options.body = JSON.stringify(body);
  }
  const response = await fetch(path, options);
  const result = await response.json();
  if (!response.ok) {
    throw new Error(result.error || 'The operation failed.');
  }
  return result;
}

function updateRecipient() {
  const recipient = getElement('recipient').value;
  getElement('chat-recipient').textContent = recipient === '*' ? 'To everyone' : 'To ' + recipient;
  getElement('file-recipient').textContent = recipient === '*'
    ? 'Choose one recipient to send a file.'
    : 'File recipient: ' + recipient;
  for (const button of getElement('user-list').querySelectorAll('button')) {
    button.setAttribute('aria-pressed', String(button.dataset.username === recipient));
  }
}

function updateUsers(users) {
  const select = getElement('recipient');
  const selected = select.value;
  select.replaceChildren();
  getElement('user-list').replaceChildren();
  getElement('user-count').textContent = String(users.length);

  for (const name of ['*', ...users]) {
    const label = name === '*' ? 'Everyone' : name;
    const option = document.createElement('option');
    option.value = name;
    option.textContent = label;
    select.append(option);

    const item = document.createElement('li');
    const button = createElement('button', 'user-button', label);
    button.type = 'button';
    button.dataset.username = name;
    button.disabled = !connected;
    button.onclick = () => {
      select.value = name;
      updateRecipient();
    };
    item.append(button);
    getElement('user-list').append(item);
  }

  // Giữ người đang chọn nếu người đó vẫn còn trong danh sách.
  if (selected === '*' || users.includes(selected)) {
    select.value = selected;
  }
  updateRecipient();
}

function appendMessage(item) {
  const messages = getElement('messages');
  messages.append(item);
  // Bỏ tin cũ để trang không giữ quá nhiều phần tử khi chạy lâu.
  while (messages.children.length > 500) {
    messages.firstChild.remove();
  }
  messages.scrollTop = messages.scrollHeight;
}

function showMessage(sender, recipient, text, outgoing = false) {
  const item = createElement('li', outgoing ? 'message message-outgoing' : 'message');
  const target = recipient === '*' ? 'everyone' : recipient;
  const kind = recipient === '*' ? 'Broadcast' : 'Private';
  let details = sender + ' → ' + target + ' · ' + kind;
  if (outgoing) {
    details += ' · Send requested';
  }
  item.append(createElement('span', 'message-meta', details));
  item.append(createElement('span', 'message-body', text));
  appendMessage(item);
}

function updateByteCount() {
  const size = new TextEncoder().encode(getElement('text').value).length;
  const counter = getElement('byte-count');
  counter.textContent = size + ' / 480 UTF-8 bytes';
  counter.classList.toggle('over-limit', size > 480);
}

function formatBytes(bytes) {
  if (bytes >= 1024 * 1024) {
    return (bytes / (1024 * 1024)).toFixed(2) + ' MiB';
  }
  if (bytes >= 1024) {
    return (bytes / 1024).toFixed(2) + ' KiB';
  }
  return bytes + ' B';
}

function createFileRow(event) {
  if (fileRows.size >= 100) {
    for (const [key, row] of fileRows) {
      if (row.done) {
        row.item.remove();
        fileRows.delete(key);
        break;
      }
    }
  }

  const row = {
    item: createElement('li', 'file-card'),
    peer: createElement('span', 'file-peer'),
    name: createElement('strong', 'file-name'),
    status: createElement('span', 'badge'),
    progress: document.createElement('progress'),
    details: createElement('span', 'file-details'),
    warning: createElement('p', 'file-warning'),
    button: createElement('button', 'cancel-button', 'Cancel'),
    done: false
  };
  row.button.type = 'button';
  row.button.onclick = async () => {
    try {
      await api('/api/cancel', {direction: event.direction, transfer_id: event.transfer_id});
    } catch (error) {
      showError(error);
    }
  };
  const header = createElement('div', 'file-header');
  header.append(row.peer, row.status);
  const footer = createElement('div', 'file-footer');
  footer.append(row.details, row.button);
  row.item.append(header, row.name, row.progress, footer, row.warning);
  getElement('files').append(row.item);
  return row;
}

function updateFile(event) {
  const key = event.direction + ':' + event.transfer_id;
  if (!fileRows.has(key)) {
    fileRows.set(key, createFileRow(event));
  }
  const row = fileRows.get(key);
  const bytes = event.direction === 'send' ? event.acknowledged_bytes : event.received_bytes;
  const total = event.total_bytes;
  row.done = terminalStatuses.has(event.status);
  row.name.textContent = event.name || event.transfer_id.slice(0, 8);
  row.peer.textContent = event.direction === 'send' ? 'To ' + event.recipient : 'From ' + event.sender;
  row.status.textContent = statusLabels[event.status] || event.status;

  let statusClass = 'status-active';
  if (event.status === 'verifying' || event.status === 'completed' || event.status === 'failed') {
    statusClass = 'status-' + event.status;
  } else if (row.done) {
    statusClass = '';
  }
  row.status.className = 'badge ' + statusClass;

  // Đủ byte vẫn phải đợi kết quả SHA-256. Chỉ event completed mới báo hoàn tất.
  row.progress.max = total || 1;
  row.progress.value = total === 0 && event.status === 'completed' ? 1 : bytes;
  row.progress.setAttribute('aria-label', 'Transfer progress for ' + row.name.textContent);
  let percent = 0;
  if (total > 0) {
    percent = Math.min(100, Math.floor(bytes * 100 / total));
  } else if (total === 0 && event.status === 'completed') {
    percent = 100;
  }
  const totalLabel = total == null ? '?' : formatBytes(total);
  row.details.textContent = formatBytes(bytes) + ' / ' + totalLabel + ' · ' + percent + '%';
  row.details.title = bytes + ' / ' + (total ?? '?') + ' bytes';

  const warnings = [];
  if (event.cleanup_pending) {
    warnings.push('Temporary file cleanup is pending.');
  }
  if (event.cancel_delivery === 'failed') {
    warnings.push('Could not notify the server about cancellation.');
  }
  if (event.confirmation_delivery === 'failed') {
    warnings.push('File saved, but confirmation could not be sent.');
  }
  if (event.message) {
    warnings.push(event.message);
  }
  row.warning.textContent = warnings.join(' ');
  row.button.disabled = row.done || !connected;
}

function handleEvent(event) {
  switch (event.type) {
    case 'message':
      showMessage(event.sender, event.recipient, event.text);
      break;
    case 'error':
      // Lỗi hiện tại lấy từ snapshot; không phát lại lỗi trong lịch sử.
      break;
    case 'file':
      updateFile(event);
      break;
    case 'connection':
      username = event.username || '';
      updateConnection(event.connected);
      break;
    case 'users':
      updateUsers(event.users);
      break;
  }
}

async function login(event) {
  event.preventDefault();
  busy = true;
  updateConnection(false);
  clearError('login');
  try {
    const name = getElement('username').value;
    await api('/api/login', {
      host: getElement('host').value,
      port: Number(getElement('port').value),
      username: name,
      password: getElement('password').value
    });
    username = name;
    updateConnection(true);
  } catch (error) {
    showError(error, 'login');
  } finally {
    busy = false;
    getElement('password').value = '';
    updateConnection(connected);
  }
}

async function logout() {
  try {
    await api('/api/logout', {});
    updateConnection(false);
  } catch (error) {
    showError(error);
  }
}

async function sendMessage(event) {
  event.preventDefault();
  const text = getElement('text').value;
  const recipient = getElement('recipient').value;
  if (new TextEncoder().encode(text).length > 480) {
    showError('Messages must not exceed 480 UTF-8 bytes.', 'chat');
    return;
  }
  try {
    await api('/api/messages', {recipient, text});
    clearError('chat');
    showMessage('You', recipient, text, true);
    getElement('text').value = '';
    updateByteCount();
  } catch (error) {
    showError(error, 'chat');
  }
}

async function uploadFile(event) {
  event.preventDefault();
  const file = getElement('file').files[0];
  const recipient = getElement('recipient').value;
  if (!file || recipient === '*') {
    showError('Choose a file and one recipient.', 'file');
    return;
  }
  if (file.size > 16 * 1024 * 1024) {
    showError('Files must not exceed 16 MiB.', 'file');
    return;
  }

  uploading = true;
  getElement('send-file').disabled = true;
  getElement('upload-status').textContent = 'Uploading to the local client…';
  try {
    const query = 'recipient=' + encodeURIComponent(recipient) + '&name=' + encodeURIComponent(file.name);
    const response = await fetch('/api/files?' + query, {
      method: 'POST',
      headers: {'X-Client-Token': token},
      body: file
    });
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.error || 'The upload failed.');
    }
    getElement('upload-status').textContent = 'Send requested. Follow the transfer below.';
    sendingFile = true;
    clearError('file');
  } catch (error) {
    showError(error, 'file');
    getElement('upload-status').textContent = 'Upload did not complete.';
  } finally {
    uploading = false;
    updateConnection(connected);
  }
}

// Lấy các event mới theo cursor, sau đó đợi 500 ms trước lần hỏi tiếp theo.
async function poll() {
  try {
    const result = await api('/api/events?after=' + cursor);
    serverErrors = result.errors || [];
    sendingFile = result.sending_file;
    clearError('bridge');
    if (result.reset) {
      appendMessage(createElement('li', 'message-system', 'Some older events have expired. Showing the remaining history.'));
    }
    for (const event of result.events) {
      handleEvent(event);
    }
    cursor = result.cursor;
    if (!busy) {
      updateConnection(result.connected);
    }
  } catch (error) {
    updateConnection(false);
    showError(error, 'bridge');
  } finally {
    setTimeout(poll, 500);
  }
}

getElement('login').onsubmit = login;
getElement('logout').onclick = logout;
getElement('chat').onsubmit = sendMessage;
getElement('upload').onsubmit = uploadFile;
getElement('dismiss-error').onclick = async () => {
  const ids = serverErrors.map(error => error.id);
  try {
    await api('/api/dismiss-errors', {ids});
    serverErrors = serverErrors.filter(error => !ids.includes(error.id));
    localErrors.clear();
    renderErrors();
  } catch (error) {
    showError(error, 'bridge');
  }
};
getElement('login').oninput = () => clearError('login');
getElement('recipient').onchange = () => { updateRecipient(); clearError('file'); };
getElement('text').oninput = () => { updateByteCount(); clearError('chat'); };
getElement('file').onchange = () => {
  clearError('file');
  const file = getElement('file').files[0];
  getElement('selected-file').textContent = file ? file.name : 'No file selected';
};
updateConnection(false);
poll();
