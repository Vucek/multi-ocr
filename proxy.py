#!/usr/bin/env python3
"""Local CORS proxy for ndk.cz Kramerius API + OCR converter."""

import http.server
import ssl
import urllib.request
import urllib.error
import os
import io
import json
import threading
import subprocess
import re
import time
from urllib.parse import urlparse, parse_qs

PORT = int(os.environ.get('PORT', 3456))
NDK_HOST = 'ndk.cz'
STATIC_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(STATIC_DIR, 'output')
STATE_FILE = os.path.join(OUTPUT_DIR, 'last_job.json')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Persistent session store
session_store = {'cookie': '', 'user': None}
store_lock = threading.Lock()

# OCR job state
ocr_job = {
    'status': 'idle',   # idle, running, done, error
    'progress': 0,
    'message': '',
    'result_path': '',
    'result_format': '',
    'title': '',
}
ocr_lock = threading.Lock()

MIME_TYPES = {
    '.html': 'text/html',
    '.js': 'application/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.ico': 'image/x-icon',
}


# ---- NDK fetch helpers ----

def fetch_ndk_data(path, cookie=''):
    """Fetch data from ndk.cz with a 60-second timeout."""
    url = f'https://{NDK_HOST}{path}'
    headers = {
        'Host': NDK_HOST,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': '*/*',
        'Accept-Language': 'cs,en;q=0.9',
        'Referer': 'https://ndk.cz/',
    }
    if cookie:
        headers['Cookie'] = cookie
    req = urllib.request.Request(url, headers=headers)
    ctx = ssl.create_default_context()
    resp = urllib.request.urlopen(req, context=ctx, timeout=60)
    return resp.read(), resp.headers.get('Content-Type', '')


def fetch_ndk_retry(path, cookie='', retries=4):
    """Fetch with exponential backoff — survives transient network drops (sleep/wake)."""
    last_exc = None
    for attempt in range(retries):
        try:
            return fetch_ndk_data(path, cookie)
        except Exception as e:
            last_exc = e
            if attempt < retries - 1:
                wait = 3 * (2 ** attempt)   # 3s, 6s, 12s
                print(f'[OCR] Fetch failed ({e}), retry {attempt+1}/{retries-1} in {wait}s...')
                time.sleep(wait)
    raise last_exc


# ---- State persistence ----

def _save_ocr_state():
    """Persist essential job completion info so download survives server restarts."""
    try:
        snapshot = {}
        with ocr_lock:
            for k in ('status', 'progress', 'message', 'result_path', 'result_format', 'title'):
                snapshot[k] = ocr_job[k]
        with open(STATE_FILE, 'w') as f:
            json.dump(snapshot, f)
        print(f'[OCR] State saved → {STATE_FILE}')
    except Exception as e:
        print(f'[OCR] Warning: could not save state: {e}')


def _load_ocr_state():
    """On startup, restore a completed job if the output file still exists."""
    try:
        with open(STATE_FILE) as f:
            state = json.load(f)
        path = state.get('result_path', '')
        if state.get('status') == 'done' and path and os.path.isfile(path):
            with ocr_lock:
                ocr_job.update(state)
            print(f'[OCR] Restored completed job: {state.get("title", "")}')
            print(f'[OCR] File ready at: {path}')
        else:
            if path and not os.path.isfile(path):
                print(f'[OCR] Previous output file gone, starting fresh: {path}')
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f'[OCR] Could not load state: {e}')


# ---- OCR pipeline ----

def parse_page_range(range_str, max_pages):
    """Parse '1-10' or '1,3,5-8' into sorted list of 0-based indices."""
    if not range_str or not range_str.strip():
        return list(range(max_pages))
    indices = set()
    for part in range_str.split(','):
        part = part.strip()
        if '-' in part:
            a, b = part.split('-', 1)
            for i in range(int(a), min(int(b) + 1, max_pages + 1)):
                indices.add(i - 1)
        else:
            n = int(part)
            if 1 <= n <= max_pages:
                indices.add(n - 1)
    return sorted(indices)


def run_ocr_job(uuid, fmt, page_range_str):
    """Background thread: fetch pages from NDK, OCR, produce PDF or EPUB."""
    try:
        with ocr_lock:
            ocr_job['status'] = 'running'
            ocr_job['progress'] = 0
            ocr_job['message'] = 'Fetching book metadata...'
            ocr_job['result_path'] = ''

        with store_lock:
            cookie = session_store['cookie']

        # 1. Metadata
        meta_raw, _ = fetch_ndk_retry(f'/search/api/v5.0/item/{uuid}', cookie)
        meta = json.loads(meta_raw)
        title = meta.get('title', 'Unknown')
        with ocr_lock:
            ocr_job['title'] = title
            ocr_job['message'] = f'Found: {title}'

        # 2. Page list
        children_raw, _ = fetch_ndk_retry(f'/search/api/v5.0/item/{uuid}/children', cookie)
        all_pages = json.loads(children_raw)
        indices = parse_page_range(page_range_str, len(all_pages))
        num_pages = len(indices)
        with ocr_lock:
            ocr_job['message'] = f'Processing {num_pages} pages...'

        # 3. Download images + OCR
        from PIL import Image
        import pytesseract
        if os.path.isfile('/opt/homebrew/bin/tesseract'):
            pytesseract.pytesseract.tesseract_cmd = '/opt/homebrew/bin/tesseract'

        ocr_texts = []
        pil_images = []

        for i, page_idx in enumerate(indices):
            page = all_pages[page_idx]
            pid = page['pid']
            label = page.get('title', f'Page {page_idx + 1}')

            with ocr_lock:
                ocr_job['progress'] = int((i / num_pages) * 85)
                ocr_job['message'] = f'OCR page {i+1}/{num_pages}: {label}'

            # Try full → preview → thumb
            img_data = None
            for tier in ('full', 'preview', 'thumb'):
                try:
                    img_data, _ = fetch_ndk_retry(f'/search/api/v5.0/item/{pid}/{tier}', cookie)
                    break
                except Exception:
                    continue
            if img_data is None:
                raise RuntimeError(f'Could not fetch image for page {page_idx + 1}')

            img = Image.open(io.BytesIO(img_data))
            if img.mode != 'RGB':
                img = img.convert('RGB')
            pil_images.append(img)

            text = pytesseract.image_to_string(img, lang='ces+eng')
            ocr_texts.append((label, text))
            print(f'[OCR] Page {i+1}/{num_pages} done ({len(text)} chars)')

        with ocr_lock:
            ocr_job['progress'] = 90
            ocr_job['message'] = f'Generating {fmt.upper()}...'

        # 4. Generate output into stable OUTPUT_DIR (not a temp dir)
        safe_title = re.sub(r'[^\w\s-]', '', title)[:60].strip() or 'book'
        out_path = os.path.join(OUTPUT_DIR, f'{safe_title}.{fmt}')

        if fmt == 'pdf':
            _generate_pdf(pil_images, ocr_texts, out_path)
        else:
            _generate_epub(ocr_texts, out_path, title)

        with ocr_lock:
            ocr_job['status'] = 'done'
            ocr_job['progress'] = 100
            ocr_job['message'] = 'Conversion complete!'
            ocr_job['result_path'] = out_path
            ocr_job['result_format'] = fmt

        _save_ocr_state()
        print(f'[OCR] Done → {out_path}')

    except Exception as e:
        print(f'[OCR] Error: {e}')
        import traceback; traceback.print_exc()
        with ocr_lock:
            ocr_job['status'] = 'error'
            ocr_job['message'] = str(e)


def _generate_pdf(images, ocr_texts, out_path):
    """Generate searchable PDF: Tesseract PDF per page → merge with pikepdf."""
    import pytesseract

    if os.path.isfile('/opt/homebrew/bin/tesseract'):
        pytesseract.pytesseract.tesseract_cmd = '/opt/homebrew/bin/tesseract'

    pdf_pages = []
    for img in images:
        pdf_bytes = pytesseract.image_to_pdf_or_hocr(img, lang='ces+eng', extension='pdf')
        pdf_pages.append(pdf_bytes)

    if len(pdf_pages) == 1:
        with open(out_path, 'wb') as f:
            f.write(pdf_pages[0])
        return

    # Merge with pikepdf
    try:
        import pikepdf
        merged = pikepdf.Pdf.new()
        for page_bytes in pdf_pages:
            src = pikepdf.Pdf.open(io.BytesIO(page_bytes))
            merged.pages.extend(src.pages)
        merged.save(out_path)
        return
    except ImportError:
        pass

    # Fallback: PyPDF2
    try:
        import tempfile
        from PyPDF2 import PdfMerger
        page_files = []
        with tempfile.TemporaryDirectory() as td:
            for i, pb in enumerate(pdf_pages):
                pf = os.path.join(td, f'p{i:04d}.pdf')
                with open(pf, 'wb') as f:
                    f.write(pb)
                page_files.append(pf)
            merger = PdfMerger()
            for pf in page_files:
                merger.append(pf)
            merger.write(out_path)
            merger.close()
        return
    except ImportError:
        pass

    # Last resort: ghostscript
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        page_files = []
        for i, pb in enumerate(pdf_pages):
            pf = os.path.join(td, f'p{i:04d}.pdf')
            with open(pf, 'wb') as f:
                f.write(pb)
            page_files.append(pf)
        try:
            subprocess.run(
                ['gs', '-dBATCH', '-dNOPAUSE', '-q', '-sDEVICE=pdfwrite',
                 f'-sOutputFile={out_path}'] + page_files,
                check=True, timeout=300
            )
        except Exception:
            import shutil
            shutil.copy(page_files[0], out_path)


def _generate_epub(ocr_texts, out_path, title):
    """Generate EPUB from OCR text."""
    from ebooklib import epub

    book = epub.EpubBook()
    book.set_identifier(f'kramerius-{abs(hash(title))}')
    book.set_title(title)
    book.set_language('cs')

    chapters = []
    for i, (label, text) in enumerate(ocr_texts):
        ch = epub.EpubHtml(title=label, file_name=f'page_{i:04d}.xhtml', lang='cs')
        paragraphs = text.strip().split('\n\n')
        html_parts = []
        for p in paragraphs:
            p = p.strip()
            if p:
                p = p.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                p = p.replace('\n', '<br/>')
                html_parts.append(f'<p>{p}</p>')
        ch.content = f'<h2>{label}</h2>{"".join(html_parts)}'
        book.add_item(ch)
        chapters.append(ch)

    book.toc = [(epub.Section('Pages'), chapters)]
    book.add_item(epub.EpubNcx())
    book.add_item(epub.EpubNav())
    book.spine = ['nav'] + chapters

    css = epub.EpubItem(uid='style', file_name='style/default.css', media_type='text/css',
                        content='body{font-family:serif;line-height:1.6}h2{margin-bottom:1em}')
    book.add_item(css)
    epub.write_epub(out_path, book)


# ---- HTTP handler ----

class ProxyHandler(http.server.BaseHTTPRequestHandler):

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors_headers()
        self.end_headers()

    def do_GET(self):
        path = self.path.split('?')[0]
        if path == '/auth/status':
            self._auth_status()
        elif path == '/auth/set-cookie':
            self._auth_set_cookie()
        elif path == '/auth/clear':
            self._auth_clear()
        elif path == '/api/ocr/status':
            self._ocr_status()
        elif path.startswith('/api/ocr/download/'):
            self._ocr_download()
        elif self.path.startswith('/search/'):
            self._proxy()
        else:
            self._static()

    def do_HEAD(self):
        if self.path.startswith('/search/'):
            self._proxy(method='HEAD')
        else:
            self._static()

    def do_POST(self):
        path = self.path.split('?')[0]
        if path == '/auth/set-cookie':
            self._auth_set_cookie_post()
        elif path == '/api/ocr/start':
            self._ocr_start()
        else:
            self.send_error(404)

    # ---- Auth ----

    def _auth_status(self):
        with store_lock:
            cookie = session_store['cookie']

        result = {'has_cookie': bool(cookie), 'cookie_preview': '', 'user': None}
        if cookie:
            result['cookie_preview'] = cookie[:20] + '...' if len(cookie) > 20 else cookie
            try:
                user_data = self._fetch_ndk('/search/api/v5.0/user', cookie)
                user = json.loads(user_data)
                result['user'] = user
                with store_lock:
                    session_store['user'] = user
            except Exception as e:
                result['error'] = str(e)

        self._json_response(result)

    def _auth_set_cookie(self):
        qs = parse_qs(urlparse(self.path).query)
        cookie_val = qs.get('cookie', [''])[0]
        if cookie_val:
            with store_lock:
                session_store['cookie'] = cookie_val
            print(f'[AUTH] Cookie set via GET: {cookie_val[:30]}...')
        self._json_response({'ok': True})

    def _auth_set_cookie_post(self):
        length = int(self.headers.get('Content-Length', 0))
        raw = self.rfile.read(length) if length else b''
        try:
            data = json.loads(raw)
            cookie_val = data.get('cookie', '')
        except Exception:
            cookie_val = raw.decode().strip()
        if cookie_val:
            with store_lock:
                session_store['cookie'] = cookie_val
            print(f'[AUTH] Cookie set via POST: {cookie_val[:30]}...')
        self._json_response({'ok': True})

    def _auth_clear(self):
        with store_lock:
            session_store['cookie'] = ''
            session_store['user'] = None
        print('[AUTH] Session cleared')
        self._json_response({'ok': True})

    # ---- Proxy ----

    def _get_cookie(self):
        hdr = self.headers.get('X-NDK-Cookie', '')
        if hdr:
            return hdr
        with store_lock:
            return session_store['cookie']

    def _proxy(self, method=None):
        method = method or 'GET'
        target_url = f'https://{NDK_HOST}{self.path}'
        cookie = self._get_cookie()

        raw_accept = self.headers.get('Accept', '*/*')
        safe_accept = raw_accept.encode('ascii', 'ignore').decode('ascii') or '*/*'

        headers = {
            'Host': NDK_HOST,
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': safe_accept,
            'Accept-Language': 'cs,en;q=0.9',
            'Referer': 'https://ndk.cz/',
        }
        if cookie:
            headers['Cookie'] = cookie

        print(f'[PROXY] {method} {self.path} {"(cookie)" if cookie else "(no cookie)"}')
        req = urllib.request.Request(target_url, headers=headers, method=method)
        ctx = ssl.create_default_context()

        try:
            resp = urllib.request.urlopen(req, context=ctx, timeout=30)
            ct = resp.headers.get('Content-Type', 'application/octet-stream')
            data = resp.read() if method == 'GET' else b''
            self.send_response(resp.status)
            self._cors_headers()
            self.send_header('Content-Type', ct)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            if data:
                self.wfile.write(data)

        except urllib.error.HTTPError as e:
            body = b''
            ct = 'text/plain'
            try:
                body = e.read()
                ct = e.headers.get('Content-Type', 'text/plain')
            except Exception:
                pass
            self.send_response(e.code)
            self._cors_headers()
            self.send_header('Content-Type', ct)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        except Exception as e:
            print(f'[PROXY] Error: {e}')
            msg = json.dumps({'error': str(e)}).encode()
            self.send_response(502)
            self._cors_headers()
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)

    def _fetch_ndk(self, path, cookie=''):
        url = f'https://{NDK_HOST}{path}'
        headers = {'Host': NDK_HOST, 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
        if cookie:
            headers['Cookie'] = cookie
        req = urllib.request.Request(url, headers=headers)
        ctx = ssl.create_default_context()
        resp = urllib.request.urlopen(req, context=ctx, timeout=15)
        return resp.read().decode()

    # ---- OCR ----

    def _ocr_start(self):
        length = int(self.headers.get('Content-Length', 0))
        raw = self.rfile.read(length) if length else b''
        try:
            data = json.loads(raw)
        except Exception:
            data = {}

        uuid = data.get('uuid', '')
        fmt = data.get('format', 'pdf')
        pages = data.get('pages', '')

        if not uuid:
            self._json_response({'error': 'No UUID provided'}, 400)
            return

        with ocr_lock:
            if ocr_job['status'] == 'running':
                self._json_response({'error': 'A conversion is already in progress'}, 409)
                return
            ocr_job['status'] = 'running'
            ocr_job['progress'] = 0
            ocr_job['message'] = 'Starting...'
            ocr_job['result_path'] = ''
            ocr_job['result_format'] = fmt

        t = threading.Thread(target=run_ocr_job, args=(uuid, fmt, pages), daemon=True)
        t.start()
        self._json_response({'ok': True, 'message': 'OCR job started'})

    def _ocr_status(self):
        with ocr_lock:
            self._json_response({
                'status': ocr_job['status'],
                'progress': ocr_job['progress'],
                'message': ocr_job['message'],
                'format': ocr_job.get('result_format', 'pdf'),
            })

    def _ocr_download(self):
        with ocr_lock:
            path = ocr_job.get('result_path', '')
            fmt = ocr_job.get('result_format', 'pdf')
            title = ocr_job.get('title', 'book')

        print(f'[DOWNLOAD] path={path!r} exists={os.path.isfile(path) if path else False}')

        if not path or not os.path.isfile(path):
            self.send_error(404, 'No file ready')
            return

        ct = 'application/pdf' if fmt == 'pdf' else 'application/epub+zip'
        ascii_title = title.encode('ascii', 'ignore').decode('ascii')
        safe_ascii = re.sub(r'[^\w\s-]', '', ascii_title)[:60].strip() or 'book'
        import urllib.parse
        _clean = re.sub(r'[^\w\s.-]', '', title)[:60].strip() or 'book'
        filename_utf8 = urllib.parse.quote(f'{_clean}.{fmt}')

        with open(path, 'rb') as f:
            data = f.read()

        self.send_response(200)
        self._cors_headers()
        self.send_header('Content-Type', ct)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Content-Disposition',
                         f'attachment; filename="{safe_ascii}.{fmt}"; filename*=UTF-8\'\'{filename_utf8}')
        self.end_headers()
        self.wfile.write(data)

    # ---- Helpers ----

    def _json_response(self, obj, status=200):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self._cors_headers()
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _static(self):
        path = self.path.split('?')[0]
        if path == '/':
            path = '/app.html'
        elif path == '/debug':
            path = '/index.html'

        filepath = os.path.normpath(os.path.join(STATIC_DIR, path.lstrip('/')))
        if not filepath.startswith(STATIC_DIR):
            self.send_error(403)
            return
        if not os.path.isfile(filepath):
            self.send_error(404)
            return

        ext = os.path.splitext(filepath)[1]
        ct = MIME_TYPES.get(ext, 'application/octet-stream')
        with open(filepath, 'rb') as f:
            data = f.read()

        self.send_response(200)
        self.send_header('Content-Type', ct)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, HEAD, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, X-NDK-Cookie, Cookie')
        self.send_header('Access-Control-Expose-Headers',
                         'X-Set-Cookie, X-Redirect-Url, Content-Length, Content-Type')

    def log_message(self, fmt, *args):
        pass


class ThreadedHTTPServer(http.server.HTTPServer):
    def process_request(self, request, client_address):
        t = threading.Thread(target=self.process_request_thread,
                             args=(request, client_address))
        t.daemon = True
        t.start()

    def process_request_thread(self, request, client_address):
        try:
            self.finish_request(request, client_address)
        except Exception:
            self.handle_error(request, client_address)
        finally:
            self.shutdown_request(request)


if __name__ == '__main__':
    _load_ocr_state()
    server = ThreadedHTTPServer(('', PORT), ProxyHandler)
    print(f'\n  Kramerius OCR Converter running at http://localhost:{PORT}')
    print(f'  Output directory: {OUTPUT_DIR}')
    print(f'  Debug interface:  http://localhost:{PORT}/debug\n')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nShutting down.')
        server.shutdown()
