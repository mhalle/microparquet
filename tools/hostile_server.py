# Usage: hostile_server.py dir port mode  -- modes: norange, truncate, nosuffix, redirect:<port>, echo
import http.server, os, sys, re, json
root, port, mode = sys.argv[1], int(sys.argv[2]), sys.argv[3]
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        if mode.startswith('redirect:'):
            self.send_response(302); self.send_header('Location', f'http://localhost:{mode.split(":")[1]}{self.path}'); self.end_headers(); return
        if mode == 'echo' and self.path.endswith('/headers'):
            body = json.dumps(dict(self.headers)).encode(); self.send_response(200); self.send_header('Content-Length', str(len(body))); self.end_headers(); self.wfile.write(body); return
        p = os.path.join(root, os.path.basename(self.path))
        data = open(p, 'rb').read(); n = len(data)
        if mode == 'echo': open('/tmp/echo_headers.json', 'a').write(json.dumps(dict(self.headers)) + '\n')
        m = re.match(r'bytes=(\d*)-(\d*)', self.headers.get('Range', ''))
        if mode == 'norange' or not m: self.send_response(200); body = data
        else:
            a, b = m.groups()
            if a == '':
                if mode == 'nosuffix': self.send_error(501); return
                a, b = max(0, n - int(b)), n - 1
            else: a, b = int(a), min(int(b) if b else n - 1, n - 1)
            body = data[a:b + 1]
            if mode == 'truncate' and len(body) > 100: body = body[:len(body) // 2]
            self.send_response(206); self.send_header('Content-Range', f'bytes {a}-{b}/{n}')
        self.send_header('Content-Length', str(len(body))); self.end_headers(); self.wfile.write(body)
http.server.ThreadingHTTPServer(('127.0.0.1', port), H).serve_forever()
