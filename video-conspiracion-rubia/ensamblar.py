"""
═══════════════════════════════════════════════════════════════════
LA CONSPIRACIÓN RUBIA — une las escenas de Seedance en un solo MP4
═══════════════════════════════════════════════════════════════════

USO:
    python ensamblar.py                  # descarga, normaliza, une y pone el texto del cierre
    python ensamblar.py --sin-texto      # igual, pero sin el texto del cierre
    python ensamblar.py --solo-descargar # solo baja los clips a salida/clips/

REQUISITOS:
    pip install imageio-ffmpeg pillow    # imageio-ffmpeg trae su propio ffmpeg

RED:
    Necesita acceso a d3u0tzju9qaucj.cloudfront.net (donde Higgsfield guarda los videos).

Lee escenas.json (misma carpeta). Salida: salida/conspiracion_rubia_final.mp4
═══════════════════════════════════════════════════════════════════
"""

import sys
import json
import time
import shutil
import argparse
import subprocess
import urllib.request
from datetime import datetime
from pathlib import Path

# ════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════

CARPETA = Path(__file__).resolve().parent
ESCENAS_JSON = CARPETA / "escenas.json"
SALIDA = CARPETA / "salida"

ANCHO, ALTO, FPS = 720, 1280, 30   # vertical 9:16, igual que Seedance 720p

FUENTES = [
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
    "/Library/Fonts/Arial Bold.ttf",
    "C:/Windows/Fonts/arialbd.ttf",
]


def log(msg, prefix="→"):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {prefix}  {msg}", flush=True)


def ffmpeg_exe():
    exe = shutil.which("ffmpeg")
    if exe:
        return exe
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except ImportError:
        log("No encuentro ffmpeg. Instala con: pip install imageio-ffmpeg", "❌")
        sys.exit(1)


def correr(cmd):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        log(r.stderr[-2000:], "❌")
        sys.exit(1)
    return r


def tiene_audio(ff, video):
    r = subprocess.run([ff, "-hide_banner", "-i", str(video)], capture_output=True, text=True)
    return "Audio:" in r.stderr


def descargar(url, destino, intentos=4):
    for i in range(1, intentos + 1):
        try:
            with urllib.request.urlopen(url, timeout=120) as resp, open(destino, "wb") as f:
                shutil.copyfileobj(resp, f)
            return
        except Exception as e:
            log(f"Intento {i}/{intentos} falló para {destino.name}: {e}", "⚠️")
            if i == intentos:
                raise
            time.sleep(2 ** i)


def normalizar(ff, entrada, salida):
    """Mismo tamaño, fps, códec y audio en todos los clips para poder unirlos sin recodificar."""
    vf = (f"scale={ANCHO}:{ALTO}:force_original_aspect_ratio=decrease,"
          f"pad={ANCHO}:{ALTO}:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1,fps={FPS},format=yuv420p")
    cmd = [ff, "-y", "-hide_banner", "-i", str(entrada)]
    if tiene_audio(ff, entrada):
        # apad + -shortest: el audio dura exactamente lo mismo que el video (sin desfase al unir)
        cmd += ["-map", "0:v:0", "-map", "0:a:0",
                "-af", "loudnorm=I=-16:TP=-1.5:LRA=11,aresample=48000,apad"]
    else:
        cmd += ["-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000",
                "-map", "0:v:0", "-map", "1:a:0"]
    cmd += ["-shortest", "-vf", vf, "-c:v", "libx264", "-preset", "medium", "-crf", "18",
            "-c:a", "aac", "-b:a", "192k", "-ac", "2", "-ar", "48000",
            "-movflags", "+faststart", str(salida)]
    correr(cmd)


def dibujar_linea(linea, fuente, destino):
    """Una línea de texto centrada en un PNG transparente del tamaño del video, con sombra suave."""
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
    letra = ImageFont.truetype(fuente, linea["tamano"])
    pos = (ANCHO // 2, linea["y"])
    sombra = Image.new("RGBA", (ANCHO, ALTO), (0, 0, 0, 0))
    ImageDraw.Draw(sombra).text((pos[0] + 2, pos[1] + 4), linea["texto"], font=letra,
                                anchor="mt", fill=(0, 0, 0, 150))
    capa = sombra.filter(ImageFilter.GaussianBlur(4))
    ImageDraw.Draw(capa).text(pos, linea["texto"], font=letra, anchor="mt",
                              fill=(255, 248, 236, 255), stroke_width=2, stroke_fill=(60, 40, 90, 255))
    capa.save(destino)


def poner_texto(ff, entrada, salida, lineas, fuente):
    """Texto del cierre en el tercio superior (Seedance dejó ese espacio libre), con fade-in.
    Se dibuja con Pillow y se superpone con overlay: el ffmpeg de imageio-ffmpeg no trae drawtext."""
    cmd = [ff, "-y", "-hide_banner", "-i", str(entrada)]
    cadena, anterior = [], "0:v"
    for i, linea in enumerate(lineas):
        png = SALIDA / f"texto_{i}.png"
        dibujar_linea(linea, fuente, png)
        cmd += ["-loop", "1", "-i", str(png)]
        cadena.append(f"[{i + 1}:v]format=rgba,fade=t=in:st={linea['aparece']}:d=0.5:alpha=1[t{i}]")
        cadena.append(f"[{anterior}][t{i}]overlay=0:0:shortest=1[v{i}]")
        anterior = f"v{i}"
    cmd += ["-filter_complex", ";".join(cadena), "-map", f"[{anterior}]", "-map", "0:a:0",
            "-c:v", "libx264", "-preset", "medium", "-crf", "18", "-pix_fmt", "yuv420p",
            "-r", str(FPS), "-c:a", "copy", "-movflags", "+faststart", str(salida)]
    correr(cmd)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sin-texto", action="store_true")
    ap.add_argument("--solo-descargar", action="store_true")
    ap.add_argument("--fuente", help="Ruta a un .ttf para el texto del cierre")
    args = ap.parse_args()

    datos = json.loads(ESCENAS_JSON.read_text(encoding="utf-8"))
    escenas = [e for e in datos["escenas"] if e.get("usar", True)]
    faltan = [e["n"] for e in escenas if not e.get("video_url")]
    if faltan:
        log(f"Estas escenas no tienen video_url todavía: {faltan}", "❌")
        sys.exit(1)

    ff = ffmpeg_exe()
    (SALIDA / "clips").mkdir(parents=True, exist_ok=True)
    (SALIDA / "normalizados").mkdir(parents=True, exist_ok=True)

    crudos = []
    for e in escenas:
        destino = SALIDA / "clips" / f"{e['n']:02d}_{e['slug']}.mp4"
        if not destino.exists():
            log(f"Descargando escena {e['n']} ({e['titulo']})", "📥")
            descargar(e["video_url"], destino)
        crudos.append((e, destino))
    log(f"{len(crudos)} clips en {SALIDA / 'clips'}", "✅")
    if args.solo_descargar:
        return

    normalizados = []
    for e, crudo in crudos:
        norm = SALIDA / "normalizados" / crudo.name
        log(f"Normalizando escena {e['n']}", "🎞️")
        normalizar(ff, crudo, norm)
        normalizados.append((e, norm))

    cierre = datos.get("cierre", {})
    ultimo_e, ultimo = normalizados[-1]
    if not args.sin_texto and ultimo_e.get("es_cierre") and cierre.get("lineas"):
        fuente = args.fuente or next((f for f in FUENTES if Path(f).exists()), None)
        if fuente:
            con_texto = SALIDA / "normalizados" / f"{ultimo.stem}_texto.mp4"
            log(f"Poniendo el texto del cierre ({Path(fuente).name})", "✍️")
            poner_texto(ff, ultimo, con_texto, cierre["lineas"], fuente)
            normalizados[-1] = (ultimo_e, con_texto)
        else:
            log("No encontré una fuente .ttf; uso --fuente RUTA. Sigo sin texto.", "⚠️")

    # Filtro concat (recodifica): alinea audio y video en cada corte, así la voz no se desfasa al final
    final = SALIDA / "conspiracion_rubia_final.mp4"
    log("Uniendo todo", "🔗")
    cmd = [ff, "-y", "-hide_banner"]
    for _, p in normalizados:
        cmd += ["-i", str(p)]
    n = len(normalizados)
    cadena = "".join(f"[{i}:v][{i}:a]" for i in range(n)) + f"concat=n={n}:v=1:a=1[v][a]"
    cmd += ["-filter_complex", cadena, "-map", "[v]", "-map", "[a]",
            "-c:v", "libx264", "-preset", "medium", "-crf", "18", "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart", str(final)]
    correr(cmd)

    total = sum(e["duracion"] for e, _ in normalizados)
    log(f"Listo: {final} (~{total} s)", "🎬")


if __name__ == "__main__":
    main()
