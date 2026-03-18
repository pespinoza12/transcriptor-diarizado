# -*- coding: utf-8 -*-
"""
Transcriptor Diarizado - Servidor Web
======================================
Aplicacion web para transcribir audios con identificacion de hablantes.
Usa Gemini para transcripcion + diarizacion inteligente.

Ejecutar: python app.py
"""

import os
import sys
import json
import time
import shutil
import logging
import secrets
import threading
from pathlib import Path
from datetime import datetime
from functools import wraps
from flask import (Flask, render_template, request, jsonify, send_from_directory,
                   session, redirect, url_for)
from flask_cors import CORS
import subprocess

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transcriptor_diarizado.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Cargar variables de entorno
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))
CORS(app)

# Configuracion
APP_PASSWORD = os.environ.get("APP_PASSWORD", "transcriptor2024")
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "500"))
app.config['MAX_CONTENT_LENGTH'] = MAX_UPLOAD_MB * 1024 * 1024

# Configuracion de carpetas
BASE_DIR = Path(__file__).parent
INPUT_FOLDER = BASE_DIR / "whatsapp_audios"
OUTPUT_FOLDER = BASE_DIR / "processados"

# Crear carpetas si no existen
INPUT_FOLDER.mkdir(exist_ok=True)
OUTPUT_FOLDER.mkdir(exist_ok=True)

# Estado global del procesamiento
processing_state = {
    "is_running": False,
    "should_stop": False,
    "current_file": None,
    "progress": 0,
    "total_files": 0,
    "processed_files": 0,
    "logs": [],
    "last_transcription": None
}

# Lock para thread-safety
state_lock = threading.Lock()


# ============ AUTENTICACION ============

def login_required(f):
    """Decorator para proteger rutas."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('authenticated'):
            if request.is_json or request.path.startswith('/api/'):
                return jsonify({"error": "No autorizado"}), 401
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Pagina de login."""
    if session.get('authenticated'):
        return redirect(url_for('index'))

    error = None
    if request.method == 'POST':
        password = request.form.get('password', '')
        if password == APP_PASSWORD:
            session['authenticated'] = True
            session.permanent = True
            return redirect(url_for('index'))
        else:
            error = "Clave incorrecta"

    return render_template('login.html', error=error)


@app.route('/logout')
def logout():
    """Cerrar sesion."""
    session.clear()
    return redirect(url_for('login'))


# ============ FUNCIONES DE AUDIO ============

def add_log(message, level="info"):
    """Agrega un mensaje al log."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    with state_lock:
        processing_state["logs"].append({
            "time": timestamp,
            "level": level,
            "message": message
        })
        if len(processing_state["logs"]) > 100:
            processing_state["logs"] = processing_state["logs"][-100:]

    if level == "error":
        logging.error(message)
    else:
        logging.info(message)


def get_audio_files():
    """Obtiene la lista de archivos de audio en la carpeta de entrada."""
    audio_extensions = ('.mp3', '.wav', '.ogg', '.opus', '.m4a', '.mp4', '.mkv', '.webm')
    files = []

    if INPUT_FOLDER.exists():
        for f in INPUT_FOLDER.iterdir():
            if f.suffix.lower() in audio_extensions:
                files.append({
                    "name": f.name,
                    "size": f.stat().st_size,
                    "size_mb": round(f.stat().st_size / (1024 * 1024), 2)
                })

    return sorted(files, key=lambda x: x["name"])


def convert_to_mp3(file_path):
    """Convierte el archivo a MP3 si es necesario."""
    file_path = Path(file_path)

    if file_path.suffix.lower() == '.mp3':
        return str(file_path)

    mp3_path = file_path.with_suffix('.mp3')

    add_log(f"Convirtiendo {file_path.name} a MP3...")

    cmd = ['ffmpeg', '-y', '-i', str(file_path), '-q:a', '0', '-map', 'a', str(mp3_path)]

    try:
        result = subprocess.run(cmd, capture_output=True, timeout=300)
        if result.returncode == 0 and mp3_path.exists():
            add_log(f"Conversion exitosa: {mp3_path.name}")
            return str(mp3_path)
        else:
            add_log(f"Error en conversion: {result.stderr.decode()[:200]}", "error")
            return None
    except Exception as e:
        add_log(f"Error convirtiendo: {e}", "error")
        return None


def get_audio_duration(file_path):
    """Obtiene la duracion del audio en segundos usando ffprobe."""
    cmd = [
        'ffprobe', '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        str(file_path)
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=30)
        if result.returncode == 0:
            return float(result.stdout.decode().strip())
    except:
        pass
    return 0


def split_audio(file_path, segment_duration=480):
    """Divide un audio largo en segmentos mas pequenos."""
    file_path = Path(file_path)
    duration = get_audio_duration(file_path)

    if duration <= segment_duration:
        return [str(file_path)]

    num_segments = int(duration / segment_duration) + 1
    add_log(f"Audio largo ({int(duration/60)}min). Dividiendo en {num_segments} partes...")

    segments = []
    temp_dir = file_path.parent / "temp_segments"
    temp_dir.mkdir(exist_ok=True)

    for i in range(num_segments):
        start_time = i * segment_duration
        segment_path = temp_dir / f"{file_path.stem}_parte{i+1}.mp3"

        cmd = [
            'ffmpeg', '-y',
            '-i', str(file_path),
            '-ss', str(start_time),
            '-t', str(segment_duration),
            '-q:a', '0',
            str(segment_path)
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, timeout=120)
            if result.returncode == 0 and segment_path.exists():
                segments.append(str(segment_path))
                add_log(f"  Parte {i+1}/{num_segments} creada")
            else:
                add_log(f"Error creando segmento {i+1}", "error")
        except Exception as e:
            add_log(f"Error dividiendo audio: {e}", "error")

    return segments


def cleanup_segments(segments, original_path):
    """Limpia los archivos temporales de segmentos."""
    for seg in segments:
        seg_path = Path(seg)
        if seg_path != Path(original_path) and seg_path.exists():
            try:
                os.remove(seg)
            except:
                pass

    temp_dir = Path(original_path).parent / "temp_segments"
    if temp_dir.exists():
        try:
            temp_dir.rmdir()
        except:
            pass


def transcribe_with_gemini(audio_path, config, time_offset=0, previous_context=""):
    """Transcribe un archivo de audio usando Gemini con diarizacion."""
    import google.generativeai as genai

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise ValueError("GEMINI_API_KEY no configurada")

    genai.configure(api_key=api_key)

    tipo_audio = config.get("tipo_audio", "telemarketing")
    num_participantes = config.get("num_participantes", 2)
    nombres = config.get("nombres", {})

    if tipo_audio == "telemarketing":
        context = """Este es un audio de una llamada de TELEMARKETING/CALL CENTER.

## IDENTIFICACION DE ROLES
- Hay exactamente 2 participantes: OPERADOR y CLIENTE
- El OPERADOR es quien realiza la llamada (trabaja en el call center)
- El CLIENTE es quien recibe la llamada

## COMO IDENTIFICARLOS
- El OPERADOR generalmente:
  * Habla primero o se presenta
  * Menciona el nombre de la empresa
  * Hace preguntas de venta, retencion o encuesta
  * Usa frases como "le llamo de...", "mi nombre es..."

- El CLIENTE generalmente:
  * Responde preguntas
  * Puede estar confundido al inicio
  * Respuestas mas cortas al principio"""

        if nombres.get("operador"):
            context += f"\n\nNOTA: El nombre del operador es: {nombres['operador']}"

    elif tipo_audio == "reunion":
        context = f"""Este es un audio de una REUNION.

## IDENTIFICACION DE PARTICIPANTES
- Hay aproximadamente {num_participantes} participantes
- Identifica a cada persona por su NOMBRE REAL cuando sea posible
- Si alguien dice "Hola Maria" o "Pedro, que opinas?", usa esos nombres
- Si no puedes identificar el nombre, usa: Participante 1, Participante 2, etc."""

        if nombres:
            nombres_str = ", ".join([f"{k}: {v}" for k, v in nombres.items() if v])
            if nombres_str:
                context += f"\n\nNOTA: Nombres conocidos: {nombres_str}"

    else:
        context = f"""Este es un audio de una CONVERSACION INFORMAL.

## IDENTIFICACION DE PARTICIPANTES
- Hay aproximadamente {num_participantes} participantes
- Identifica a cada persona por su nombre cuando sea posible
- Si no puedes identificar nombres, usa: Persona 1, Persona 2, etc."""

    continuity_section = ""
    if previous_context:
        continuity_section = f"\n\n{previous_context}\n\n"

    prompt = f"""{context}
{continuity_section}
## FORMATO DE SALIDA
Usa este formato exacto:

[MM:SS] **NOMBRE/ROL**: Texto transcrito...

Ejemplos:
[00:00] **OPERADOR**: Buenas tardes, mi nombre es Maria de EnelX...
[00:15] **CLIENTE**: Hola, si digame...

## REGLAS
1. Incluye timestamps cada vez que cambie el hablante
2. Transcribe TODO el audio completo, no resumas
3. Manten fidelidad al contenido original
4. Si hay audio inaudible, indica [inaudible]
5. Si hay ruido o interferencia, indica [ruido]
6. Transcribe en el idioma original del audio

Ahora transcribe el audio completo:"""

    add_log("Subiendo audio a Gemini...")

    try:
        audio_file = genai.upload_file(audio_path)

        while audio_file.state.name == "PROCESSING":
            add_log("Procesando audio en Gemini...")
            time.sleep(2)
            audio_file = genai.get_file(audio_file.name)

        if audio_file.state.name == "FAILED":
            raise Exception("Error procesando archivo en Gemini")

        add_log("Generando transcripcion...")

        model = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            generation_config={
                "temperature": 0.1,
                "max_output_tokens": 8192,
            }
        )

        response = model.generate_content(
            [prompt, audio_file],
            request_options={"timeout": 600}
        )

        transcription_text = response.text

        try:
            genai.delete_file(audio_file.name)
        except:
            pass

        transcription_json = parse_transcription_to_json(transcription_text)

        return {
            "text": transcription_text,
            "json": transcription_json,
            "config": config
        }

    except Exception as e:
        add_log(f"Error en Gemini: {e}", "error")
        raise


def clean_transcription(text):
    """Limpia la transcripcion consolidando secciones de ruido consecutivas."""
    import re

    lines = text.strip().split('\n')
    cleaned_lines = []
    noise_start = None
    noise_end = None
    noise_count = 0

    noise_patterns = [
        r'\[(\d{2}:\d{2})\]\s*\*\*\[?rui[di]o\]?\*\*',
        r'\[(\d{2}:\d{2})\]\s*\*\*\[?noise\]?\*\*',
        r'\[(\d{2}:\d{2})\]\s*\*\*\[?silencio\]?\*\*',
        r'\[(\d{2}:\d{2})\]\s*\*\*\[?inaudible\]?\*\*',
        r'\[(\d{2}:\d{2})\]\s*\*\*\[rui[di]o\]\*\*:\s*\[rui[di]o\]',
    ]

    def is_noise_line(line):
        line_lower = line.lower()
        for pattern in noise_patterns:
            if re.match(pattern, line_lower):
                return True
        return False

    def get_timestamp(line):
        match = re.match(r'\[(\d{2}:\d{2})\]', line)
        if match:
            return match.group(1)
        return None

    def timestamp_to_seconds(ts):
        if not ts:
            return 0
        parts = ts.split(':')
        return int(parts[0]) * 60 + int(parts[1])

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if is_noise_line(line):
            ts = get_timestamp(line)
            if ts:
                if noise_start is None:
                    noise_start = ts
                noise_end = ts
                noise_count += 1
        else:
            if noise_start is not None and noise_count > 0:
                start_secs = timestamp_to_seconds(noise_start)
                end_secs = timestamp_to_seconds(noise_end)
                duration_secs = end_secs - start_secs

                if noise_count >= 3:
                    duration_mins = duration_secs // 60
                    duration_text = f"aprox. {duration_mins} minutos" if duration_mins > 0 else f"aprox. {duration_secs} segundos"

                    if duration_secs > 10:
                        cleaned_lines.append(
                            f"[{noise_start} - {noise_end}] **[RUIDO/ESPERA]**: Audio sin conversacion audible ({duration_text})"
                        )
                else:
                    cleaned_lines.append(f"[{noise_start}] **[ruido]**")

                noise_start = None
                noise_end = None
                noise_count = 0

            cleaned_lines.append(line)

    if noise_start is not None and noise_count > 0:
        start_secs = timestamp_to_seconds(noise_start)
        end_secs = timestamp_to_seconds(noise_end)
        duration_secs = end_secs - start_secs

        if noise_count >= 3 and duration_secs > 10:
            duration_mins = duration_secs // 60
            duration_text = f"aprox. {duration_mins} minutos" if duration_mins > 0 else f"aprox. {duration_secs} segundos"
            cleaned_lines.append(
                f"[{noise_start} - {noise_end}] **[RUIDO/ESPERA]**: Audio sin conversacion audible ({duration_text})"
            )

    return '\n'.join(cleaned_lines)


def parse_transcription_to_json(text):
    """Convierte la transcripcion de texto a formato JSON estructurado."""
    import re

    lines = text.strip().split('\n')
    segments = []

    pattern = r'\[(\d{2}:\d{2})\]\s*\*\*([^*]+)\*\*:\s*(.+)'

    for line in lines:
        match = re.match(pattern, line.strip())
        if match:
            segments.append({
                "timestamp": match.group(1),
                "speaker": match.group(2).strip(),
                "text": match.group(3).strip()
            })

    return {
        "segments": segments,
        "total_segments": len(segments),
        "speakers": list(set(s["speaker"] for s in segments))
    }


def extract_speaker_context(transcription_text, config):
    """Extrae informacion sobre los hablantes para mantener consistencia entre partes."""
    import re

    pattern = r'\*\*([^*]+)\*\*:'
    speakers = re.findall(pattern, transcription_text)
    unique_speakers = list(dict.fromkeys(speakers))

    if not unique_speakers:
        return ""

    speaker_samples = {}
    lines = transcription_text.split('\n')

    for speaker in unique_speakers:
        for line in lines:
            if f"**{speaker}**:" in line:
                match = re.search(r'\*\*' + re.escape(speaker) + r'\*\*:\s*(.+)', line)
                if match and speaker not in speaker_samples:
                    speaker_samples[speaker] = match.group(1)[:100]
                    break

    tipo_audio = config.get("tipo_audio", "telemarketing")

    if tipo_audio == "telemarketing":
        context = "IMPORTANTE - CONTINUIDAD DE HABLANTES:\nEsta es una CONTINUACION del mismo audio. Los hablantes ya fueron identificados en la parte anterior:\n"
        for speaker, sample in speaker_samples.items():
            context += f'- {speaker}: dijo cosas como "{sample}..."\n'
        context += "\nDEBES mantener EXACTAMENTE los mismos roles (OPERADOR/CLIENTE) para las mismas voces.\nNO cambies quien es OPERADOR y quien es CLIENTE."
    else:
        context = "IMPORTANTE - CONTINUIDAD DE HABLANTES:\nEsta es una CONTINUACION del mismo audio. Los participantes ya identificados son:\n"
        for speaker, sample in speaker_samples.items():
            context += f'- {speaker}: dijo cosas como "{sample}..."\n'
        context += "\nDEBES mantener los MISMOS nombres/identificadores para las mismas voces.\nNO asignes nuevos nombres a voces que ya fueron identificadas."

    return context


def adjust_timestamps(text, offset_seconds):
    """Ajusta los timestamps en una transcripcion agregando un offset."""
    import re

    def add_offset(match):
        time_str = match.group(1)
        mins, secs = map(int, time_str.split(':'))
        total_secs = mins * 60 + secs + offset_seconds
        new_mins = total_secs // 60
        new_secs = total_secs % 60
        return f"[{new_mins:02d}:{new_secs:02d}]"

    return re.sub(r'\[(\d{2}:\d{2})\]', add_offset, text)


def transcribe_long_audio(audio_path, config):
    """Transcribe un audio, dividiendolo en partes si es muy largo."""
    duration = get_audio_duration(audio_path)
    segment_duration = 480

    if duration <= segment_duration:
        return transcribe_with_gemini(audio_path, config)

    add_log(f"Audio de {int(duration/60)} minutos detectado. Procesando por partes...")

    segments = split_audio(audio_path, segment_duration)

    if not segments:
        raise Exception("No se pudieron crear segmentos del audio")

    all_transcriptions = []
    all_json_segments = []
    previous_context = ""

    for i, segment_path in enumerate(segments):
        offset_seconds = i * segment_duration
        add_log(f"Transcribiendo parte {i+1}/{len(segments)} (desde {offset_seconds//60}:{offset_seconds%60:02d})...")

        try:
            result = transcribe_with_gemini(
                segment_path,
                config,
                time_offset=0,
                previous_context=previous_context if i > 0 else ""
            )

            if i == 0:
                previous_context = extract_speaker_context(result["text"], config)
                add_log(f"  Hablantes identificados: {result['json'].get('speakers', [])}")

            if offset_seconds > 0:
                adjusted_text = adjust_timestamps(result["text"], offset_seconds)
            else:
                adjusted_text = result["text"]

            all_transcriptions.append(adjusted_text)

            for seg in result["json"]["segments"]:
                mins, secs = map(int, seg["timestamp"].split(':'))
                total_secs = mins * 60 + secs + offset_seconds
                seg["timestamp"] = f"{total_secs // 60:02d}:{total_secs % 60:02d}"
                all_json_segments.append(seg)

        except Exception as e:
            add_log(f"Error en parte {i+1}: {e}", "error")
            all_transcriptions.append(f"\n[Error en parte {i+1}: {e}]\n")

    cleanup_segments(segments, audio_path)

    return {
        "text": "\n".join(all_transcriptions),
        "json": {
            "segments": all_json_segments,
            "total_segments": len(all_json_segments),
            "speakers": list(set(s["speaker"] for s in all_json_segments)),
            "parts_processed": len(segments),
            "total_duration_minutes": int(duration / 60)
        },
        "config": config
    }


def process_files(config):
    """Procesa todos los archivos de audio."""
    global processing_state

    with state_lock:
        processing_state["is_running"] = True
        processing_state["should_stop"] = False
        processing_state["processed_files"] = 0
        processing_state["logs"] = []

    add_log("Iniciando procesamiento...")

    audio_files = get_audio_files()

    if not audio_files:
        add_log("No hay archivos para procesar", "error")
        with state_lock:
            processing_state["is_running"] = False
        return

    with state_lock:
        processing_state["total_files"] = len(audio_files)

    add_log(f"Encontrados {len(audio_files)} archivos para procesar")

    for i, file_info in enumerate(audio_files):
        with state_lock:
            if processing_state["should_stop"]:
                add_log("Procesamiento detenido por el usuario")
                break

            processing_state["current_file"] = file_info["name"]
            processing_state["progress"] = int((i / len(audio_files)) * 100)

        file_path = INPUT_FOLDER / file_info["name"]
        add_log(f"Procesando [{i+1}/{len(audio_files)}]: {file_info['name']}")

        try:
            mp3_path = convert_to_mp3(file_path)

            if mp3_path is None:
                add_log(f"No se pudo convertir {file_info['name']}", "error")
                continue

            add_log("Usando Gemini (diarizado)")
            result = transcribe_long_audio(mp3_path, config)
            cleaned_text = clean_transcription(result["text"])
            add_log("Transcripcion limpiada (ruido consolidado)")

            # Guardar resultados
            output_name = Path(file_info["name"]).stem
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            text_file = OUTPUT_FOLDER / f"{output_name}_{timestamp}_transcripcion.txt"
            with open(text_file, 'w', encoding='utf-8') as f:
                f.write(f"Archivo: {file_info['name']}\n")
                f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Tipo: {config.get('tipo_audio', 'telemarketing')}\n")
                f.write("=" * 60 + "\n\n")
                f.write(cleaned_text)

            json_file = OUTPUT_FOLDER / f"{output_name}_{timestamp}_transcripcion.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "archivo": file_info["name"],
                    "fecha": datetime.now().isoformat(),
                    "config": config,
                    "transcripcion": result["json"]
                }, f, ensure_ascii=False, indent=2)

            add_log(f"Guardado: {text_file.name}")

            # Mover archivo original a procesados
            dest_path = OUTPUT_FOLDER / file_info["name"]
            if dest_path.exists():
                dest_path = OUTPUT_FOLDER / f"{output_name}_{timestamp}{file_path.suffix}"

            shutil.move(str(file_path), str(dest_path))
            add_log(f"Movido a procesados: {dest_path.name}")

            if mp3_path != str(file_path) and Path(mp3_path).exists():
                try:
                    os.remove(mp3_path)
                except:
                    pass

            with state_lock:
                processing_state["processed_files"] += 1
                processing_state["last_transcription"] = cleaned_text

        except Exception as e:
            add_log(f"Error procesando {file_info['name']}: {e}", "error")
            logging.exception(e)

    with state_lock:
        processing_state["is_running"] = False
        processing_state["current_file"] = None
        processing_state["progress"] = 100

    add_log(f"Procesamiento completado: {processing_state['processed_files']}/{len(audio_files)} archivos")


# ============ RUTAS DE LA API ============

@app.route('/')
@login_required
def index():
    """Pagina principal."""
    return render_template('index.html')


@app.route('/api/status')
@login_required
def get_status():
    """Obtiene el estado actual del procesamiento."""
    with state_lock:
        return jsonify({
            "is_running": processing_state["is_running"],
            "current_file": processing_state["current_file"],
            "progress": processing_state["progress"],
            "total_files": processing_state["total_files"],
            "processed_files": processing_state["processed_files"],
            "logs": processing_state["logs"][-20:],
            "last_transcription": processing_state["last_transcription"]
        })


@app.route('/api/files')
@login_required
def get_files():
    """Obtiene la lista de archivos pendientes."""
    files = get_audio_files()
    return jsonify({
        "files": files,
        "total": len(files)
    })


@app.route('/api/upload/start', methods=['POST'])
@login_required
def upload_start():
    """Inicia un upload chunked. Retorna un upload_id."""
    data = request.json or {}
    filename = Path(data.get('filename', 'audio.mp3')).name
    total_chunks = data.get('total_chunks', 1)

    upload_id = secrets.token_hex(8)
    temp_dir = BASE_DIR / "temp_uploads" / upload_id
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Guardar metadata
    meta = {"filename": filename, "total_chunks": total_chunks, "received": 0}
    with open(temp_dir / "meta.json", 'w') as f:
        json.dump(meta, f)

    return jsonify({"upload_id": upload_id})


@app.route('/api/upload/chunk', methods=['POST'])
@login_required
def upload_chunk():
    """Sube un chunk de archivo."""
    upload_id = request.form.get('upload_id')
    chunk_index = int(request.form.get('chunk_index', 0))

    if not upload_id or 'chunk' not in request.files:
        return jsonify({"error": "Datos incompletos"}), 400

    temp_dir = BASE_DIR / "temp_uploads" / upload_id
    if not temp_dir.exists():
        return jsonify({"error": "Upload no encontrado"}), 404

    # Guardar chunk
    chunk = request.files['chunk']
    chunk.save(str(temp_dir / f"chunk_{chunk_index:04d}"))

    # Actualizar metadata
    meta_path = temp_dir / "meta.json"
    with open(meta_path) as f:
        meta = json.load(f)
    meta["received"] = chunk_index + 1
    with open(meta_path, 'w') as f:
        json.dump(meta, f)

    return jsonify({"received": chunk_index + 1, "total": meta["total_chunks"]})


@app.route('/api/upload/complete', methods=['POST'])
@login_required
def upload_complete():
    """Ensambla los chunks en el archivo final."""
    data = request.json or {}
    upload_id = data.get('upload_id')

    if not upload_id:
        return jsonify({"error": "upload_id requerido"}), 400

    temp_dir = BASE_DIR / "temp_uploads" / upload_id
    if not temp_dir.exists():
        return jsonify({"error": "Upload no encontrado"}), 404

    meta_path = temp_dir / "meta.json"
    with open(meta_path) as f:
        meta = json.load(f)

    filename = meta["filename"]
    dest = INPUT_FOLDER / filename

    # Ensamblar chunks
    with open(dest, 'wb') as out:
        for i in range(meta["total_chunks"]):
            chunk_path = temp_dir / f"chunk_{i:04d}"
            if not chunk_path.exists():
                return jsonify({"error": f"Chunk {i} faltante"}), 400
            with open(chunk_path, 'rb') as inp:
                out.write(inp.read())

    # Limpiar temp
    shutil.rmtree(str(temp_dir), ignore_errors=True)

    add_log(f"Archivo subido: {filename} ({dest.stat().st_size / (1024*1024):.1f} MB)")

    return jsonify({"filename": filename, "size_mb": round(dest.stat().st_size / (1024*1024), 2)})


@app.route('/api/start', methods=['POST'])
@login_required
def start_processing():
    """Inicia el procesamiento de archivos."""
    with state_lock:
        if processing_state["is_running"]:
            return jsonify({"error": "Ya hay un procesamiento en curso"}), 400

    config = request.json or {}

    thread = threading.Thread(target=process_files, args=(config,))
    thread.daemon = True
    thread.start()

    return jsonify({"message": "Procesamiento iniciado"})


@app.route('/api/stop', methods=['POST'])
@login_required
def stop_processing():
    """Detiene el procesamiento."""
    with state_lock:
        processing_state["should_stop"] = True

    return jsonify({"message": "Deteniendo procesamiento..."})


@app.route('/api/results')
@login_required
def get_results():
    """Lista archivos de resultados disponibles para descarga."""
    results = []
    if OUTPUT_FOLDER.exists():
        for f in sorted(OUTPUT_FOLDER.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
            if f.suffix in ('.txt', '.json'):
                results.append({
                    "name": f.name,
                    "size_mb": round(f.stat().st_size / (1024 * 1024), 2),
                    "date": datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
                })
    return jsonify({"results": results[:50]})


@app.route('/api/download/<filename>')
@login_required
def download_file(filename):
    """Descarga un archivo de resultado."""
    return send_from_directory(str(OUTPUT_FOLDER), filename, as_attachment=True)


if __name__ == '__main__':
    print("=" * 60)
    print("TRANSCRIPTOR DIARIZADO")
    print("=" * 60)
    print(f"Carpeta de entrada: {INPUT_FOLDER}")
    print(f"Carpeta de salida:  {OUTPUT_FOLDER}")
    print(f"Password: {APP_PASSWORD}")
    print("-" * 60)

    port = int(os.environ.get("PORT", 5050))
    print(f"Servidor en http://0.0.0.0:{port}")
    print("=" * 60)

    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
