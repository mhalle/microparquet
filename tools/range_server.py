# Usage: range_server.py dir port [latency_ms] -- static files with HTTP Range support and optional latency
import http.server, os, sys, time, re
root, port = sys.argv[1], int(sys.argv[2]); lat = float(sys.argv[3]) / 1000 if len(sys.argv) > 3 else 0
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        time.sleep(lat)
        p = os.path.join(root, os.path.basename(self.path))
        if not os.path.isfile(p): self.send_error(404); return
        data = open(p, 'rb').read(); n = len(data)
        m = re.match(r'bytes=(\d*)-(\d*)', self.headers.get('Range', ''))
        if not m: self.send_response(200); body = data
        else:
            a, b = m.groups()
            if a == '': a, b = max(0, n - int(b)), n - 1
            else: a, b = int(a), min(int(b) if b else n - 1, n - 1)
            body = data[a:b + 1]; self.send_response(206); self.send_header('Content-Range', f'bytes {a}-{b}/{n}')
        self.send_header('Content-Length', str(len(body))); self.end_headers(); self.wfile.write(body)
http.server.ThreadingHTTPServer(('127.0.0.1', port), H).serve_forever()
