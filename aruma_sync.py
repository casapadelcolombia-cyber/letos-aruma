"""
═══════════════════════════════════════════════════════════════════
ARUMA SYNC — descarga ventas de ProSalon y las envía a Google Sheet
═══════════════════════════════════════════════════════════════════

USO:
    python aruma_sync.py            # Sincronización REAL (sin GUI)
    python aruma_sync.py --debug    # Real con navegador visible
    python aruma_sync.py --test     # Solo verifica el login

REQUISITOS:
    pip install playwright requests
    python -m playwright install chromium
═══════════════════════════════════════════════════════════════════
"""

import sys
import os
import json
import time
import argparse
import re
from datetime import datetime
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════

USUARIO     = os.environ.get("ARUMA_USER", "900798690")
CONTRASENA  = os.environ.get("ARUMA_PASSWORD", "ITALIA61")

LOGIN_URL = "http://13.84.173.88:9084/"
VENTAS_URL = "http://13.84.173.88:9084/Portal/Consultas/Proveedor_Ventas.aspx?Form=Ventas"

APPS_SCRIPT_URL = os.environ.get(
    "ARUMA_APPS_SCRIPT_URL",
    "https://script.google.com/macros/s/AKfycbzfzU1DeP0ed-67R_9nwU-HIzW4C-xOs2aewC9wmqSeiLtBP4-3ULbiCNq0XWC3cHUXFQ/exec"
)

DOWNLOAD_DIR = Path("./aruma_downloads")

MESES = {
    "Jan":"01","Feb":"02","Mar":"03","Apr":"04","May":"05","Jun":"06",
    "Jul":"07","Aug":"08","Sep":"09","Oct":"10","Nov":"11","Dec":"12"
}


def log(msg, prefix="→"):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {prefix}  {msg}", flush=True)


def parse_fecha(s):
    s = (s or "").strip()
    if not s:
        return ""
    s = re.sub(r'\s+', ' ', s)
    parts = s.split(' ')
    if len(parts) < 3:
        return ""
    mes = MESES.get(parts[0], "")
    if not mes:
        return ""
    dia = parts[1].zfill(2)
    anio = parts[2]
    return f"{anio}-{mes}-{dia}"


def parse_archivo(path):
    ventas = []
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        with open(path, "r", encoding="latin-1") as f:
            lines = f.readlines()
    
    if not lines:
        return ventas
    
    header = [h.strip() for h in lines[0].strip().split("|")]
    log(f"  Columnas detectadas: {len(header)}")
    
    idx = {h:i for i,h in enumerate(header)}
    col_bodega = idx.get("Desc. C.O.", 1)
    col_item = idx.get("ITEM", 2)
    col_desc = idx.get("DESCRIPCION", 3)
    col_unidades = idx.get("UNIDADES", 5)
    col_subtotal = idx.get("SUBTOTAL", 6)
    col_marca = idx.get("MARCA", 7)
    col_fecha = idx.get("FECHA", 12)
    
    for line in lines[1:]:
        line = line.strip()
        if not line:
            continue
        cells = line.split("|")
        if len(cells) < 13:
            continue
        try:
            unidades = int(cells[col_unidades]) if cells[col_unidades] else 0
            subtotal = int(re.sub(r'[^\d-]', '', cells[col_subtotal]) or 0)
        except (ValueError, IndexError):
            continue
        
        fecha = parse_fecha(cells[col_fecha])
        if not fecha:
            continue
        
        ventas.append({
            "fecha": fecha,
            "sku": cells[col_item].strip(),
            "descripcion": cells[col_desc].strip(),
            "bodega": cells[col_bodega].strip(),
            "unidades": unidades,
            "subtotal": subtotal,
            "marca": cells[col_marca].strip() if col_marca < len(cells) else ""
        })
    
    return ventas


def agregar_dias(ventas):
    d = {}
    for v in ventas:
        f = v["fecha"]
        if f not in d:
            d[f] = {"fecha": f, "unidades": 0, "subtotal": 0, "facturas": 0}
        d[f]["unidades"] += v["unidades"]
        d[f]["subtotal"] += v["subtotal"]
        d[f]["facturas"] += 1
    return sorted(d.values(), key=lambda x: x["fecha"])


def agregar_productos(ventas):
    p = {}
    for v in ventas:
        sku = v["sku"]
        if not sku:
            continue
        if sku not in p:
            p[sku] = {"sku": sku, "descripcion": v["descripcion"], "unidades": 0, "subtotal": 0}
        p[sku]["unidades"] += v["unidades"]
        p[sku]["subtotal"] += v["subtotal"]
    return sorted(p.values(), key=lambda x: -x["unidades"])


# ════════════════════════════════════════════════════════════════
# SCRAPER
# ════════════════════════════════════════════════════════════════

class ArumaScraper:
    def __init__(self, headless=True):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
    
    def __enter__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless)
        self.context = self.browser.new_context(
            accept_downloads=True,
            ignore_https_errors=True
        )
        self.page = self.context.new_page()
        self.page.set_default_timeout(30000)
        return self
    
    def __exit__(self, *args):
        try: self.context.close()
        except: pass
        try: self.browser.close()
        except: pass
        try: self.playwright.stop()
        except: pass
    
    def login(self):
        log("Abriendo página de login...")
        self.page.goto(LOGIN_URL, wait_until="domcontentloaded")
        time.sleep(2)
        log(f"URL actual: {self.page.url}")
        
        try:
            user_input = None
            for sel in ["input[type='text']:visible", "input[name*='user' i]:visible", 
                        "input[name*='Login' i]:visible", "input[id*='user' i]:visible"]:
                if self.page.locator(sel).count() > 0:
                    user_input = self.page.locator(sel).first
                    break
            
            pass_input = None
            for sel in ["input[type='password']:visible", "input[name*='pass' i]:visible"]:
                if self.page.locator(sel).count() > 0:
                    pass_input = self.page.locator(sel).first
                    break
            
            if not user_input or not pass_input:
                log("⚠ No se encontraron inputs de login", "⚠")
                # Imprimir HTML para debug
                if not self.headless:
                    log(f"HTML: {self.page.content()[:500]}")
                return False
            
            user_input.fill(USUARIO)
            log(f"Usuario tipeado: {USUARIO}")
            pass_input.fill(CONTRASENA)
            log("Contraseña tipeada")
            
            btn = self.page.locator(
                "input[type='submit']:visible, button[type='submit']:visible, "
                "input[value*='ngresar' i]:visible, input[value*='ogin' i]:visible, "
                "button:has-text('Ingresar'):visible, button:has-text('Login'):visible"
            ).first
            btn.click()
            log("Click en Ingresar...")
            
            time.sleep(4)
            log(f"URL post-login: {self.page.url}")
            
            if self.page.locator("input[type='password']:visible").count() > 0:
                log("⚠ Aún hay campo password - login falló?", "⚠")
                return False
            
            log("✓ Login OK", "✓")
            return True
        except Exception as e:
            log(f"Error en login: {e}", "⚠")
            return False
    
    def descargar_ventas(self):
        log("Navegando a Ventas...")
        self.page.goto(VENTAS_URL, wait_until="domcontentloaded")
        time.sleep(3)
        log(f"URL: {self.page.url}")
        
        try:
            DOWNLOAD_DIR.mkdir(exist_ok=True)
            for f in DOWNLOAD_DIR.glob("*"):
                try: f.unlink()
                except: pass
            
            # ESTRATEGIA: extraer cookies + viewstate y hacer POST con requests
            # ASP.NET requiere ViewState, EventValidation, etc. para el postback
            log("Extrayendo cookies de sesión...")
            cookies = self.context.cookies()
            cookies_dict = {c["name"]: c["value"] for c in cookies}
            log(f"  {len(cookies_dict)} cookies obtenidas")
            
            # Extraer campos hidden del form de la página actual
            log("Extrayendo campos hidden del form...")
            hidden_fields = self.page.evaluate("""
                () => {
                    const inputs = document.querySelectorAll('input[type=hidden]');
                    const data = {};
                    inputs.forEach(i => { if(i.name) data[i.name] = i.value; });
                    return data;
                }
            """)
            log(f"  {len(hidden_fields)} campos hidden")
            
            # Agregar el campo del botón Exportar
            hidden_fields["ctl00$MainContent$btnExportar"] = "Exportar"
            
            # Hacer el POST con requests usando las cookies
            log("Enviando POST a la URL de ventas...")
            session = requests.Session()
            for k, v in cookies_dict.items():
                session.cookies.set(k, v)
            
            user_agent = self.page.evaluate("() => navigator.userAgent")
            
            res = session.post(
                VENTAS_URL,
                data=hidden_fields,
                headers={
                    "User-Agent": user_agent,
                    "Referer": VENTAS_URL,
                    "Origin": LOGIN_URL.rstrip("/"),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                },
                timeout=180,
                allow_redirects=True
            )
            
            log(f"  Respuesta HTTP: {res.status_code}, {len(res.content)} bytes")
            
            # Verificar Content-Disposition
            cd = res.headers.get("Content-Disposition", "") or res.headers.get("content-disposition", "")
            content_type = res.headers.get("Content-Type", "")
            log(f"  Content-Type: {content_type}")
            log(f"  Content-Disposition: {cd or '(vacío)'}")
            
            if cd and ("attachment" in cd.lower() or "filename" in cd.lower()):
                m = re.search(r'filename[^;=\n]*=(?:UTF-8\'\')?(["\']?)([^"\';\n]*)\1', cd)
                filename = m.group(2) if m else f"ventas_{int(time.time())}.txt"
                target_path = DOWNLOAD_DIR / filename
                with open(target_path, "wb") as f:
                    f.write(res.content)
                log(f"✓ Guardado: {filename} ({len(res.content)} bytes)", "✓")
                return target_path
            
            # Si no hay attachment, ver si el contenido parece ser el archivo (texto con pipes)
            preview = res.content[:500].decode("utf-8", errors="replace")
            log(f"  Preview: {preview[:300]}")
            
            if "C.O." in preview and "|" in preview and "FECHA" in preview:
                # Es el archivo aunque sin Content-Disposition
                filename = f"ventas_{int(time.time())}.txt"
                target_path = DOWNLOAD_DIR / filename
                with open(target_path, "wb") as f:
                    f.write(res.content)
                log(f"✓ Guardado (sin attachment header): {filename}", "✓")
                return target_path
            
            log("⚠ La respuesta no contiene un archivo de ventas", "⚠")
            return None
            
        except Exception as e:
            log(f"Error descargando: {e}", "⚠")
            import traceback
            log(traceback.format_exc())
            return None


# ════════════════════════════════════════════════════════════════
# ENVIAR
# ════════════════════════════════════════════════════════════════

def enviar_a_sheet(ventas):
    if not ventas:
        log("Sin ventas para enviar", "⚠")
        return False
    
    dias = agregar_dias(ventas)
    productos = agregar_productos(ventas)
    
    payload = {
        "action": "save",
        "ventas": ventas,
        "dias": dias,
        "productos": productos
    }
    
    total_uds = sum(v["unidades"] for v in ventas)
    total_rev = sum(v["subtotal"] for v in ventas)
    log(f"Enviando: {len(ventas)} ventas, {len(productos)} productos, {len(dias)} días, {total_uds} uds, ${total_rev:,}", "📤")
    
    try:
        res = requests.post(APPS_SCRIPT_URL, json=payload, timeout=180)
        log(f"Respuesta: {res.status_code} {res.text[:200]}")
        return res.status_code == 200
    except Exception as e:
        log(f"Error enviando: {e}", "⚠")
        return False


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════

def sincronizar(headless=True):
    with ArumaScraper(headless=headless) as scraper:
        if not scraper.login():
            return False
        archivo = scraper.descargar_ventas()
        if not archivo:
            return False
    
    log(f"Parseando archivo: {archivo.name}")
    ventas = parse_archivo(archivo)
    log(f"  {len(ventas)} ventas parseadas")
    
    if not ventas:
        log("⚠ No se parsearon ventas", "⚠")
        return False
    
    return enviar_a_sheet(ventas)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Solo verifica login")
    parser.add_argument("--debug", action="store_true", help="Navegador visible")
    args = parser.parse_args()
    
    headless = not args.debug
    
    if args.test:
        with ArumaScraper(headless=headless) as scraper:
            sys.exit(0 if scraper.login() else 1)
    
    sys.exit(0 if sincronizar(headless=headless) else 1)


if __name__ == "__main__":
    main()
