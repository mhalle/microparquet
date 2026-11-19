import json, os, sys, sqlite3
root, out = sys.argv[1], sys.argv[2]
if os.path.exists(out): os.remove(out)
db = sqlite3.connect(out)
db.executescript("""
PRAGMA page_size=4096; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;
CREATE TABLE protocol(id INTEGER PRIMARY KEY, name TEXT UNIQUE, definition TEXT);
CREATE TABLE code(id INTEGER PRIMARY KEY, protocol_id INT, keycode TEXT, pronto TEXT);
CREATE TABLE name(id INTEGER PRIMARY KEY, name TEXT UNIQUE);
CREATE TABLE codeset(id INTEGER PRIMARY KEY, hash TEXT UNIQUE);
CREATE TABLE codeset_cmd(codeset_id INT, ord INT, name_id INT, code_id INT, PRIMARY KEY(codeset_id, ord)) WITHOUT ROWID;
CREATE TABLE device(id INTEGER PRIMARY KEY, manufacturer TEXT, model TEXT, codeset_id INT, meta TEXT);
""")
ids = {}
def get(tbl, key, ins):
    d = ids.setdefault(tbl, {})
    if key not in d:
        d[key] = len(d) + 1
        ins(d[key])
    return d[key]
pdir = os.path.join(root, 'protocols')
for f in sorted(os.listdir(pdir)):
    if f == 'index.json': continue
    s = open(os.path.join(pdir, f)).read(); j = json.loads(s)
    nm = j.get('name') or f[:-5]
    get('p', nm, lambda i: db.execute('INSERT INTO protocol VALUES(?,?,?)', (i, nm, s)))
for d in sorted(os.listdir(os.path.join(root, 'codesets'))):
    for f in sorted(os.listdir(os.path.join(root, 'codesets', d))):
        h = f[:-5]
        csid = get('cs', h, lambda i: db.execute('INSERT INTO codeset VALUES(?,?)', (i, h)))
        rows = []
        for o, c in enumerate(json.load(open(os.path.join(root, 'codesets', d, f)))['commands']):
            pn = c.get('protocol')
            pid = get('p', pn, lambda i: db.execute('INSERT INTO protocol VALUES(?,?,?)', (i, pn, None)))
            k = (pid, c.get('keycode'))
            cid = get('c', k, lambda i: db.execute('INSERT INTO code VALUES(?,?,?,?)', (i, pid, c.get('keycode'), c.get('pronto'))))
            nid = get('n', c.get('name'), lambda i: db.execute('INSERT INTO name VALUES(?,?)', (i, c.get('name'))))
            rows.append((csid, o, nid, cid))
        db.executemany('INSERT INTO codeset_cmd VALUES(?,?,?,?)', rows)
ddir = os.path.join(root, 'devices')
for m in sorted(os.listdir(ddir)):
    mp = os.path.join(ddir, m)
    if not os.path.isdir(mp): continue
    for f in sorted(os.listdir(mp)):
        if f == 'index.json': continue
        j = json.load(open(os.path.join(mp, f)))
        cs = j.pop('codeset', None)
        csid = ids['cs'][os.path.basename(cs)[:-5]] if cs else None
        man, mod = j.pop('manufacturer', m), j.pop('model', f[:-5])
        db.execute('INSERT INTO device(manufacturer,model,codeset_id,meta) VALUES(?,?,?,?)',
                   (man, mod, csid, json.dumps(j, separators=(',', ':'), sort_keys=True)))
db.execute('CREATE INDEX device_mm ON device(manufacturer, model)')
db.commit(); db.execute('VACUUM'); db.close()
print({k: len(v) for k, v in ids.items()})
