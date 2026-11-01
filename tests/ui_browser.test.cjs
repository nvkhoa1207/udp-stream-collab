// Chạy app.js với DOM/storage tối thiểu; không cần package npm hoặc browser.
// Kiểm tra hành vi reload, dữ liệu lưu và nút hủy, không chỉ kiểm tra chuỗi code.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {test} = require('node:test');
const source = fs.readFileSync(path.join(__dirname, '../ui/app.js'), 'utf8');

class Element {
  constructor() {
    this.children = [];
    this.value = '';
    this.dataset = {};
    this.classList = {toggle() {}};
  }
  append(...items) { this.children.push(...items); }
  replaceChildren(...items) { this.children = items; }
  setAttribute() {}
  querySelectorAll() { return []; }
}

function loadPage(storage = new Map(), blocked = false) {
  const elements = new Map();
  const get = id => {
    if (!elements.has(id)) elements.set(id, new Element());
    return elements.get(id);
  };
  get('host').value = '127.0.0.1';
  get('port').value = '9000';
  get('recipient').value = '*';
  const context = vm.createContext({
    document: {
      getElementById: get,
      querySelector: () => ({content: 'test-token'}),
      createElement: () => new Element()
    },
    localStorage: {
      getItem(key) {
        if (blocked) throw new Error('Storage denied');
        return storage.get(key) ?? null;
      },
      setItem(key, value) {
        if (blocked) throw new Error('Storage denied');
        storage.set(key, value);
      }
    },
    // Giữ poll chờ, không tạo timer/network trong unit test.
    fetch: () => new Promise(() => {}),
    setTimeout() {}, TextEncoder
  });
  vm.runInContext(source, context);
  return {get, context};
}

function input(page, id, value) {
  page.get(id).value = value;
  page.get('login').oninput({target: {id}});
}

function hasButton(element, className) {
  return element.className === className
    || element.children.some(child => hasButton(child, className));
}

test('LAN server and port survive reload; password/username/token are not stored', () => {
  const storage = new Map();
  const page = loadPage(storage);
  input(page, 'host', ' 192.168.1.50 ');
  input(page, 'port', '19000');
  input(page, 'username', 'khoa');
  input(page, 'password', 'never-store-this');
  assert.equal(storage.size, 1);
  assert.deepEqual(JSON.parse(storage.get('udp-stream-connection')),
    {host: '192.168.1.50', port: 19000});
  const reloaded = loadPage(storage);
  assert.equal(reloaded.get('host').value, '192.168.1.50');
  assert.equal(reloaded.get('port').value, '19000');
  assert.equal(reloaded.get('password').value, '');
});

test('broken JSON, invalid values or blocked storage leave form usable', () => {
  for (const saved of ['broken', 'null', '{"host":5,"port":9000}',
      '{"host":"lan","port":70000}', '{"host":"","port":9000}']) {
    const page = loadPage(new Map([['udp-stream-connection', saved]]));
    assert.equal(page.get('host').value, '127.0.0.1');
    assert.equal(page.get('port').value, '9000');
  }
  const blocked = loadPage(new Map(), true);
  assert.doesNotThrow(() => input(blocked, 'host', '192.168.1.60'));
  assert.equal(blocked.get('host').value, '192.168.1.60');
});

test('only outgoing files have Cancel, and completed outgoing files disable it', () => {
  const page = loadPage();
  vm.runInContext(`
    updateConnection(true);
    updateFile({direction:'receive', transfer_id:'receive-id', status:'receiving',
      name:'incoming.bin', sender:'hai', total_bytes:100, received_bytes:10});
    updateFile({direction:'send', transfer_id:'send-id', status:'sending',
      name:'outgoing.bin', recipient:'hai', total_bytes:100, acknowledged_bytes:10});
  `, page.context);
  const rows = page.get('files').children;
  assert.equal(hasButton(rows[0], 'cancel-button'), false);
  assert.equal(hasButton(rows[1], 'cancel-button'), true);
  assert.equal(vm.runInContext("fileRows.get('send:send-id').button.disabled", page.context), false);
  vm.runInContext(`updateFile({direction:'send', transfer_id:'send-id', status:'completed',
    total_bytes:100, acknowledged_bytes:100})`, page.context);
  assert.equal(vm.runInContext("fileRows.get('send:send-id').button.disabled", page.context), true);
});
