# La Conspiración Rubia: anuncio de Blondz Only estilo Pixar (1 minuto)

Corto animado de 1 minuto, vertical (9:16), con voces en español, para **Blondz Only Matizante Rubio Marfil** (Girlz Only). Las escenas se generaron con **Seedance 2.5 en Higgsfield**.

- **Datos de cada escena** (prompts, referencias, links y request_id): [`escenas.json`](escenas.json)
- **Script para unir el video final:** [`ensamblar.py`](ensamblar.py)

---

## 1. De dónde salió

1. **La idea:** el video de Angry Tom en X, un corto estilo Pixar de un koala de 1:23. Él dijo que lo hizo con *"Script: Sherpa by Pocket FM / Video: Seedance 2.5"*.
2. **Sherpa (Pocket FM)** solo escribe. Está hecho para radionovelas y exige entre 1.000 y 5.000 palabras por episodio, así que nunca da un guion de 1 minuto. Allí se creó la serie **"The Blonde Conspiracy"**: Maya, una rubia con el pelo naranjoso, investiga por qué todas las demás tienen un rubio perfecto. El capítulo 1 tiene 1.613 palabras y se corta cuando Maya persigue a una mujer.
3. **Aquí** ese capítulo se resumió a 1 minuto y se le agregó un final. El secreto de las rubias es Blondz Only, el matizante que Girlz Only vende en Shopify.
4. **Decisiones tomadas:**
   - Termina con el producto.
   - Formato vertical 9:16 para TikTok y Reels.
   - Frases cortas en español.
   - Estilo Pixar.
   - Calidad 720p, la máxima de la herramienta.

---

## 2. Estado de las escenas

Todas son 9:16, 720p, con audio. Ninguna la ha revisado una persona todavía, porque desde la sesión donde se generaron no se podían descargar.

| # | Escena | Tiempo | Seg. | Estado | Video |
|---|---|---|---|---|---|
| 1 | Baño: el matizante falla | 0:00–0:06 | 6 | Lista | [ver](https://d3u0tzju9qaucj.cloudfront.net/22af5e29-f0ce-49b1-a2ec-f098dd8a2f7c/ec3f6bd5-ae4f-4a3f-8ba9-d3503a58b6c3.mp4) |
| 2 | Café: todas son rubias perfectas | 0:06–0:12 | 6 | Lista | [ver](https://d3u0tzju9qaucj.cloudfront.net/22af5e29-f0ce-49b1-a2ec-f098dd8a2f7c/809efe0f-a3c0-4a72-957d-22ceb548828c.mp4) |
| 3 | La barra: "Voy donde una especialista" | 0:12–0:18 | 6 | Ver `escenas.json` | ver `escenas.json` |
| 4 | Los susurros: "Tinte de caja. Trágico." | 0:18–0:24 | 6 | Lista | [ver](https://d3u0tzju9qaucj.cloudfront.net/22af5e29-f0ce-49b1-a2ec-f098dd8a2f7c/62f89402-cfc9-4cea-bb87-581fbe8115bf.mp4) |
| 5 | Los mensajes: la mujer escribe mirándola | 0:24–0:30 | 6 | Lista | [ver](https://d3u0tzju9qaucj.cloudfront.net/22af5e29-f0ce-49b1-a2ec-f098dd8a2f7c/c93795be-67fa-46dc-9c94-073575782585.mp4) |
| 6 | La persecución por la calle | 0:30–0:37 | 7 | Lista | [ver](https://d3u0tzju9qaucj.cloudfront.net/22af5e29-f0ce-49b1-a2ec-f098dd8a2f7c/ad0d5638-625c-4ae5-9d0a-da5d17b68504.mp4) |
| 7 | El giro: "Esta es mi especialista" | 0:37–0:43 | 6 | Lista | [ver](https://d3u0tzju9qaucj.cloudfront.net/22af5e29-f0ce-49b1-a2ec-f098dd8a2f7c/b80bee53-959a-4cee-b0ba-657445720cef.mp4) |
| 8 | La transformación con Blondz Only | 0:43–0:50 | 7 | Lista | [ver](https://d3u0tzju9qaucj.cloudfront.net/22af5e29-f0ce-49b1-a2ec-f098dd8a2f7c/03d5d6c2-4853-4710-80e4-c1b589f9d73c.mp4) |
| 9 | El remate: "Tenemos una nueva" | 0:50–0:56 | 6 | Lista | [ver](https://d3u0tzju9qaucj.cloudfront.net/22af5e29-f0ce-49b1-a2ec-f098dd8a2f7c/a12b7bec-05d4-41a4-9a6d-72f64563fa2e.mp4) |
| 10 | Cierre con el producto (el texto se pone al unir) | 0:56–1:00 | 4 | Lista | [ver](https://d3u0tzju9qaucj.cloudfront.net/22af5e29-f0ce-49b1-a2ec-f098dd8a2f7c/9ac5089e-377e-4a04-a711-e55828dc0f87.mp4) |

- **Escena 3:** el primer intento lo rechazó la moderación de Higgsfield, que devolvió los créditos. Se reescribió el prompt en términos más neutros y se relanzó. El estado y el link actuales están en `escenas.json`.
- **Prueba descartada:** la primera versión de la escena 1, en 480p ([ver](https://d3u0tzju9qaucj.cloudfront.net/22af5e29-f0ce-49b1-a2ec-f098dd8a2f7c/2a800d75-0948-4840-9039-1e70979a99d9.mp4)), no se usa en el final.

---

## 3. Personajes y referencias

Para que cada personaje se vea igual en todas las escenas, primero se creó una imagen suya. Esa imagen se pasa como referencia en cada escena, con la herramienta `referencia_a_video`.

| Referencia | Imagen | Se usa en |
|---|---|---|
| **Maya (antes):** 28 años, pecas, ojos miel, suéter verde, rubio naranjoso con raíces naranjas y gancho de carey | [ver](https://d3u0tzju9qaucj.cloudfront.net/22af5e29-f0ce-49b1-a2ec-f098dd8a2f7c/a993447f-8910-4f9a-9774-5a106384c1e2.png) | escenas 1–7 |
| **Maya (después):** la misma imagen editada, con el pelo rubio marfil suelto | [ver](https://d3u0tzju9qaucj.cloudfront.net/22af5e29-f0ce-49b1-a2ec-f098dd8a2f7c/bd2c84cf-0119-48f2-8165-884872d61567.png) | escenas 8–9 |
| **La mujer del abrigo:** abrigo gris oscuro, rubio marfil liso, siempre con el celular | [ver](https://d3u0tzju9qaucj.cloudfront.net/22af5e29-f0ce-49b1-a2ec-f098dd8a2f7c/936bd006-1a29-4e42-bbbb-6f274d9b2480.png) | escenas 3, 5, 6, 7 |
| Foto real del shampoo Blondz Only | [ver](https://cdn.shopify.com/s/files/1/0072/3776/3172/products/shampoomatizante_2.jpg?v=1620142529) | escenas 7, 8 |
| Foto real del color y la textura del matizante | [ver](https://cdn.shopify.com/s/files/1/0072/3776/3172/files/08_KIT_X2_RUBIO_MARFIL_swatch_textura_azul.png?v=1779550975) | escena 8 |
| Fotos reales del kit x2 (shampoo y mascarilla) | [kit](https://cdn.shopify.com/s/files/1/0072/3776/3172/products/kit-x-2.png?v=1626722980) · [splash argán](https://cdn.shopify.com/s/files/1/0072/3776/3172/files/06_KIT_X2_RUBIO_MARFIL_producto_splash_argan.png?v=1779550975) | escena 10 |

---

## 4. Guion (1 minuto)

Hay un cambio frente a Sherpa: la mujer de la barra y la del celular son la misma persona. Así el "Voy donde una especialista" regresa como chiste en el giro.

1. **Baño (0:00–0:06).** Maya tiene la cabeza llena de espuma morada de un frasco plateado sin marca, con un temporizador corriendo en el celular. *Maya (susurrando):* "Esta vez sí… porfa." El naranja se asoma entre la espuma. *Maya:* "No, no, no…"
2. **Café (0:06–0:12).** Maya entra con el pelo en el gancho y se congela: todas las clientas tienen el mismo rubio marfil perfecto. En cámara lenta se mueven el pelo al mismo tiempo, con brillos y coro. *Maya (para sí):* "Eso es… demasiado rubio."
3. **La barra (0:12–0:18).** *Maya:* "Tu color es divino. ¿Es de salón?" *Mujer del abrigo (fría):* "Voy donde una especialista. Es privado."
4. **Los susurros (0:18–0:24).** *Rubia 1:* "¿Se lo habrá hecho ella sola?" *Rubia 2:* "Tinte de caja. Trágico." Maya se esconde detrás de la taza, roja.
5. **Los mensajes (0:24–0:30).** La mujer escribe rapidísimo en el celular sin dejar de mirarle el pelo a Maya. *Maya (pensando):* "¿A quién le estás escribiendo de mí?" La mujer sale, y Maya se para de un salto y deja el café servido.
6. **La persecución (0:30–0:37).** Maya la sigue en modo espía, escondida detrás de un poste demasiado delgado y de un bus que arranca. Un perrito le ladra. *Maya:* "¡Shhh!"
7. **El giro (0:37–0:43).** La mujer se voltea y saca del bolso un Blondz Only. *Mujer (guiño):* "Esta es mi especialista." Brillo perlado y coro celestial.
8. **La transformación (0:43–0:50).** Espuma de Blondz Only y enjuague. En cámara lenta, el pelo queda rubio marfil y brillante. *Maya:* "Por fin."
9. **El remate (0:50–0:56).** Maya vuelve al café con el pelo perfecto. Entra una chica con raíces naranjas y ahora es Maya la que escribe rapidísimo mirándola. *Maya (susurrando):* "Tenemos una nueva."
10. **Cierre (0:56–1:00).** Shampoo y mascarilla, con el texto "Blondz Only · Matizante Rubio Marfil · con Aceite de Argán · El secreto ya no es privado."

---

## 5. Cuánto costó (aproximado)

La herramienta no muestra el cobro exacto; el valor real está en el historial de créditos de Higgsfield. La estimación usa tres datos:
- Tarifa de Seedance 2.5 en Higgsfield: **6,5 créditos/s en 720p** y 3 créditos/s en 480p. Las imágenes de referencia no cuestan extra.
- Plan Plus: ~US$0,049 por crédito.
- Dólar a ~3.307 COP.

| Qué | Créditos | USD aprox. |
|---|---|---|
| 3 imágenes de personajes (2K, alta calidad) | — | menos de US$2 |
| Prueba de la escena 1 (6 s, 480p) | 18 | ~US$0,90 |
| 10 escenas en 720p (60 s) | 390 | ~US$19 |
| Escena 3, primer intento (rechazado por moderación) | 0 (devueltos) | US$0 |
| **Total hasta ahora** | **~408 + imágenes** | **~US$20–22, unos 66.000–74.000 COP** |

Rehacer una escena de 6 s en 720p cuesta unos 39 créditos, ~US$1,90.

---

## 6. Cómo seguir en una conversación nueva

### a) Quitar el bloqueo de red

En la sesión anterior, la política de red del entorno bloqueaba estos dominios:

- `d3u0tzju9qaucj.cloudfront.net`: donde Higgsfield guarda los videos e imágenes. **Sin este no se puede descargar ni unir nada.**
- `cdn.shopify.com`: fotos de los productos. Solo hace falta para verlas.

Para cambiarlo: en la barra de título de la sesión, abre el menú del entorno en la nube, elige **Edit** y cambia **Network access**. Puedes agregar esos dominios a la lista permitida o subir el nivel de acceso. Los niveles se explican en <https://code.claude.com/docs/en/claude-code-on-the-web>.

### b) Abrir la conversación nueva y pegar esto

```
Estoy continuando un proyecto de video. Lee video-conspiracion-rubia/README.md y
video-conspiracion-rubia/escenas.json de la rama claude/zealous-albattani-m8ti2e de este repo
(si no estás en esa rama: git fetch origin claude/zealous-albattani-m8ti2e && git checkout claude/zealous-albattani-m8ti2e).
Luego:
1. Verifica que puedes descargar de d3u0tzju9qaucj.cloudfront.net; si no, avísame.
2. Si la escena 3 dice "generando", consulta su request_id con estado_video de Higgsfield y actualiza escenas.json.
3. Descarga las escenas (pip install imageio-ffmpeg pillow && python video-conspiracion-rubia/ensamblar.py --solo-descargar),
   saca 2-3 fotogramas de cada una y revísalas conmigo: que Maya y la mujer del abrigo se vean iguales en todas,
   que el frasco de Blondz Only se vea bien y que los diálogos en español se entiendan.
4. Rehaz con Higgsfield (referencia_a_video, 9:16, 720p, con audio) solo las escenas que yo apruebe rehacer,
   usando el prompt y las referencias de escenas.json, y actualiza escenas.json.
5. Arma el video final con python video-conspiracion-rubia/ensamblar.py y envíamelo.
```

---

## 7. Cómo rehacer una escena

1. En `escenas.json`, toma de la escena su `prompt`, sus `referencias` y su `duracion`. Los nombres de las referencias se traducen a URL con la sección `referencias` del mismo archivo.
2. Llama a `referencia_a_video` de Higgsfield con `aspect_ratio: "9:16"`, `resolution: "720p"` y `generate_audio: true`.
3. Consulta `estado_video` con el `request_id` hasta que termine.
4. Actualiza `request_id`, `video_url` y `estado` en `escenas.json`.

Sobre la moderación: la escena 3 se rechazó una vez, probablemente por palabras como "pick-up counter" o "icy, tight smile". Si pasa, reescribe con palabras neutras. Los intentos rechazados devuelven los créditos.

---

## 8. Cómo armar el video final

```bash
pip install imageio-ffmpeg pillow
python video-conspiracion-rubia/ensamblar.py            # queda en video-conspiracion-rubia/salida/conspiracion_rubia_final.mp4
python video-conspiracion-rubia/ensamblar.py --sin-texto
python video-conspiracion-rubia/ensamblar.py --solo-descargar
```

El script:
1. Descarga los clips.
2. Los deja todos en 720×1280 a 30 fps, con el volumen parejo (−16 LUFS).
3. Los une alineando audio y video en cada corte, para que las voces no se desfasen.
4. Pone el texto del cierre con fade-in en el tercio superior de la escena 10. Los textos y posiciones están en `cierre.lineas` de `escenas.json`.

La carpeta `salida/` está en `.gitignore`. El script se probó con clips de prueba, incluyendo uno horizontal y otro sin audio, pero todavía no con los videos reales.

Otra opción es hacerlo a mano en CapCut: bajar los 10 clips en orden, unirlos y poner el texto en el último.

---

## 9. Qué revisar antes de publicar

- **Consistencia:** que Maya, la mujer del abrigo y el café se vean igual en todas las escenas.
- **Voces:** Seedance genera la voz en cada escena por separado, así que la voz de Maya puede cambiar un poco. Revisa también que los labios coincidan con el diálogo.
- **Etiqueta del producto (escenas 7, 8 y 10):** la IA a veces deforma las letras. Si el cierre queda mal, usa la foto real del producto en CapCut.
- **Color de la espuma (escena 8):** se usó de referencia la foto real de la textura del matizante. Comprueba que coincida con el producto.
- **Música (opcional):** cada escena trae su propio audio. Una canción de fondo continua en CapCut hace que todo se sienta como una sola pieza.
