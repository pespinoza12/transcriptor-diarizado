# -*- coding: utf-8 -*-
"""
Transcriptor Diarizado - Servidor Web
======================================
Transcripcion de audios con diarizacion usando Gemini.
Usuarios y transcripciones almacenados en PostgreSQL.
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
from flask import (Flask, render_template, request, jsonify,
                   session, redirect, url_for)
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
import subprocess
import psycopg2
from psycopg2.extras import RealDictCursor

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
app.secret_key = os.environ.get("SECRET_KEY", "transcriptor-diarizado-secret-key-2024-prod")
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['PERMANENT_SESSION_LIFETIME'] = 86400 * 7  # 7 dias
CORS(app)

# Configuracion
DATABASE_URL = os.environ.get("DATABASE_URL", "")
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "500"))
app.config['MAX_CONTENT_LENGTH'] = MAX_UPLOAD_MB * 1024 * 1024

# Carpetas temporales (solo para procesamiento, se borran despues)
BASE_DIR = Path(__file__).parent
INPUT_FOLDER = BASE_DIR / "whatsapp_audios"
INPUT_FOLDER.mkdir(exist_ok=True)

# Estado global del procesamiento (por usuario)
processing_states = {}
state_lock = threading.Lock()


# ============ BASE DE DATOS ============

def get_db():
    """Obtiene una conexion a la base de datos."""
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn


def init_db():
    """Crea las tablas si no existen."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS usuarios (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    nombre VARCHAR(100),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transcripciones (
                    id SERIAL PRIMARY KEY,
                    usuario_id INTEGER REFERENCES usuarios(id) ON DELETE CASCADE,
                    archivo VARCHAR(255) NOT NULL,
                    transcripcion TEXT NOT NULL,
                    tipo_audio VARCHAR(50),
                    duracion_minutos INTEGER,
                    fecha TIMESTAMP DEFAULT NOW()
                )
            """)
            # Crear usuarios iniciales si no existen
            for uname, pwd, nombre in [
                ('pedro', 'Gabriel5214!', 'Pedro'),
                ('cbascunan', 'Gabriel5214!', 'Carolina'),
            ]:
                cur.execute("SELECT id FROM usuarios WHERE username = %s", (uname,))
                if not cur.fetchone():
                    cur.execute(
                        "INSERT INTO usuarios (username, password_hash, nombre) VALUES (%s, %s, %s)",
                        (uname, generate_password_hash(pwd), nombre)
                    )
        conn.commit()
        logging.info("Base de datos inicializada")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inicializando DB: {e}")
        raise
    finally:
        conn.close()


# ============ AUTENTICACION ============

def get_user_state(user_id):
    """Obtiene o crea el estado de procesamiento para un usuario."""
    with state_lock:
        if user_id not in processing_states:
            processing_states[user_id] = {
                "is_running": False,
                "should_stop": False,
                "current_file": None,
                "progress": 0,
                "total_files": 0,
                "processed_files": 0,
                "logs": [],
                "last_transcription": None
            }
        return processing_states[user_id]


def login_required(f):
    """Decorator para proteger rutas."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user_id'):
            if request.is_json or request.path.startswith('/api/'):
                return jsonify({"error": "No autorizado"}), 401
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


@app.route('/login', methods=['GET', 'POST'])
def login():
    # Verificar sesion existente valida
    if session.get('user_id') and session.get('username'):
        return redirect(url_for('index'))

    error = None
    if request.method == 'POST':
        username = request.form.get('username', '').strip().lower()
        password = request.form.get('password', '')

        conn = get_db()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id, username, password_hash, nombre FROM usuarios WHERE username = %s", (username,))
                user = cur.fetchone()

            if user and check_password_hash(user['password_hash'], password):
                session.clear()  # Limpiar sesion vieja
                session['user_id'] = user['id']
                session['username'] = user['username']
                session['nombre'] = user['nombre']
                session.permanent = True
                return redirect(url_for('index'))
            else:
                error = "Usuario o clave incorrecta"
        finally:
            conn.close()
    else:
        # GET: limpiar sesiones invalidas (sin username = sistema viejo)
        if session.get('authenticated') and not session.get('username'):
            session.clear()

    return render_template('login.html', error=error)


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


# ============ FUNCIONES DE AUDIO ============

def add_log(user_id, message, level="info"):
    """Agrega un mensaje al log del usuario."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    state = get_user_state(user_id)
    with state_lock:
        state["logs"].append({"time": timestamp, "level": level, "message": message})
        if len(state["logs"]) > 100:
            state["logs"] = state["logs"][-100:]

    if level == "error":
        logging.error(message)
    else:
        logging.info(message)


def get_audio_files(user_id):
    """Obtiene archivos de audio del usuario."""
    user_dir = INPUT_FOLDER / str(user_id)
    audio_extensions = ('.mp3', '.wav', '.ogg', '.opus', '.m4a', '.mp4', '.mkv', '.webm')
    files = []

    if user_dir.exists():
        for f in user_dir.iterdir():
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
    cmd = ['ffmpeg', '-y', '-i', str(file_path), '-q:a', '0', '-map', 'a', str(mp3_path)]

    try:
        result = subprocess.run(cmd, capture_output=True, timeout=300)
        if result.returncode == 0 and mp3_path.exists():
            return str(mp3_path)
        return None
    except Exception:
        return None


def get_audio_duration(file_path):
    """Obtiene la duracion del audio en segundos."""
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
    """Divide un audio largo en segmentos."""
    file_path = Path(file_path)
    duration = get_audio_duration(file_path)

    if duration <= segment_duration:
        return [str(file_path)]

    num_segments = int(duration / segment_duration) + 1
    segments = []
    temp_dir = file_path.parent / "temp_segments"
    temp_dir.mkdir(exist_ok=True)

    for i in range(num_segments):
        start_time = i * segment_duration
        segment_path = temp_dir / f"{file_path.stem}_parte{i+1}.mp3"

        cmd = [
            'ffmpeg', '-y', '-i', str(file_path),
            '-ss', str(start_time), '-t', str(segment_duration),
            '-q:a', '0', str(segment_path)
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, timeout=120)
            if result.returncode == 0 and segment_path.exists():
                segments.append(str(segment_path))
        except:
            pass

    return segments


def cleanup_segments(segments, original_path):
    """Limpia archivos temporales."""
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
            shutil.rmtree(str(temp_dir), ignore_errors=True)
        except:
            pass


def transcribe_with_gemini(audio_path, config, user_id, time_offset=0, previous_context=""):
    """Transcribe un archivo de audio usando Gemini."""
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

## REGLAS
1. Incluye timestamps cada vez que cambie el hablante
2. Transcribe TODO el audio completo, no resumas
3. Manten fidelidad al contenido original
4. Si hay audio inaudible, indica [inaudible]
5. Si hay ruido o interferencia, indica [ruido]
6. Transcribe en el idioma original del audio

Ahora transcribe el audio completo:"""

    max_retries = 3

    for attempt in range(1, max_retries + 1):
        try:
            add_log(user_id, f"Subiendo audio a Gemini...{' (reintento ' + str(attempt) + ')' if attempt > 1 else ''}")

            audio_file = genai.upload_file(audio_path)

            while audio_file.state.name == "PROCESSING":
                add_log(user_id, "Procesando audio en Gemini...")
                time.sleep(2)
                audio_file = genai.get_file(audio_file.name)

            if audio_file.state.name == "FAILED":
                raise Exception("Error procesando archivo en Gemini")

            add_log(user_id, "Generando transcripcion...")

            model = genai.GenerativeModel(
                model_name="gemini-2.5-flash",
                generation_config={"temperature": 0.1, "max_output_tokens": 8192}
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

            return transcription_text

        except Exception as e:
            add_log(user_id, f"Error en Gemini (intento {attempt}/{max_retries}): {e}", "error")

            # Limpiar archivo subido si existe
            try:
                genai.delete_file(audio_file.name)
            except:
                pass

            if attempt < max_retries:
                wait = attempt * 10  # 10s, 20s
                add_log(user_id, f"Reintentando en {wait} segundos...")
                time.sleep(wait)
            else:
                add_log(user_id, f"Fallo despues de {max_retries} intentos", "error")
                raise


def clean_transcription(text):
    """Limpia la transcripcion consolidando ruido consecutivo."""
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
        return match.group(1) if match else None

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

                if noise_count >= 3 and duration_secs > 10:
                    duration_mins = duration_secs // 60
                    duration_text = f"aprox. {duration_mins} minutos" if duration_mins > 0 else f"aprox. {duration_secs} segundos"
                    cleaned_lines.append(
                        f"[{noise_start} - {noise_end}] **[RUIDO/ESPERA]**: Audio sin conversacion audible ({duration_text})"
                    )
                else:
                    cleaned_lines.append(f"[{noise_start}] **[ruido]**")

                noise_start = None
                noise_end = None
                noise_count = 0

            cleaned_lines.append(line)

    if noise_start is not None and noise_count >= 3:
        start_secs = timestamp_to_seconds(noise_start)
        end_secs = timestamp_to_seconds(noise_end)
        duration_secs = end_secs - start_secs
        if duration_secs > 10:
            duration_mins = duration_secs // 60
            duration_text = f"aprox. {duration_mins} minutos" if duration_mins > 0 else f"aprox. {duration_secs} segundos"
            cleaned_lines.append(
                f"[{noise_start} - {noise_end}] **[RUIDO/ESPERA]**: Audio sin conversacion audible ({duration_text})"
            )

    return '\n'.join(cleaned_lines)


def extract_speaker_context(transcription_text, config):
    """Extrae info de hablantes para consistencia entre partes."""
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
        context = "IMPORTANTE - CONTINUIDAD DE HABLANTES:\nEsta es una CONTINUACION del mismo audio. Los hablantes ya fueron identificados:\n"
        for speaker, sample in speaker_samples.items():
            context += f'- {speaker}: dijo cosas como "{sample}..."\n'
        context += "\nDEBES mantener EXACTAMENTE los mismos roles (OPERADOR/CLIENTE) para las mismas voces."
    else:
        context = "IMPORTANTE - CONTINUIDAD DE HABLANTES:\nEsta es una CONTINUACION del mismo audio. Los participantes ya identificados son:\n"
        for speaker, sample in speaker_samples.items():
            context += f'- {speaker}: dijo cosas como "{sample}..."\n'
        context += "\nDEBES mantener los MISMOS nombres/identificadores para las mismas voces."

    return context


def adjust_timestamps(text, offset_seconds):
    """Ajusta timestamps agregando un offset."""
    import re

    def add_offset(match):
        mins, secs = map(int, match.group(1).split(':'))
        total_secs = mins * 60 + secs + offset_seconds
        return f"[{total_secs // 60:02d}:{total_secs % 60:02d}]"

    return re.sub(r'\[(\d{2}:\d{2})\]', add_offset, text)


def transcribe_long_audio(audio_path, config, user_id):
    """Transcribe un audio, dividiendolo si es largo."""
    duration = get_audio_duration(audio_path)
    segment_duration = 480

    if duration <= segment_duration:
        text = transcribe_with_gemini(audio_path, config, user_id)
        return text, int(duration / 60)

    add_log(user_id, f"Audio de {int(duration/60)} minutos. Procesando por partes...")

    segments = split_audio(audio_path, segment_duration)
    if not segments:
        raise Exception("No se pudieron crear segmentos del audio")

    all_transcriptions = []
    previous_context = ""

    try:
        for i, segment_path in enumerate(segments):
            offset_seconds = i * segment_duration
            add_log(user_id, f"Transcribiendo parte {i+1}/{len(segments)} (desde {offset_seconds//60}:{offset_seconds%60:02d})...")

            text = transcribe_with_gemini(
                segment_path, config, user_id,
                previous_context=previous_context if i > 0 else ""
            )

            if i == 0:
                previous_context = extract_speaker_context(text, config)

            if offset_seconds > 0:
                text = adjust_timestamps(text, offset_seconds)

            all_transcriptions.append(text)
    except Exception as e:
        cleanup_segments(segments, audio_path)
        add_log(user_id, f"Fallo en parte {i+1}/{len(segments)} despues de reintentos. Transcripcion cancelada.", "error")
        raise Exception(f"Transcripcion incompleta: fallo en parte {i+1}/{len(segments)}: {e}")

    cleanup_segments(segments, audio_path)

    return "\n".join(all_transcriptions), int(duration / 60)


def process_files(config, user_id):
    """Procesa todos los archivos de audio del usuario."""
    state = get_user_state(user_id)

    with state_lock:
        state["is_running"] = True
        state["should_stop"] = False
        state["processed_files"] = 0
        state["logs"] = []

    add_log(user_id, "Iniciando procesamiento...")

    audio_files = get_audio_files(user_id)
    user_dir = INPUT_FOLDER / str(user_id)

    if not audio_files:
        add_log(user_id, "No hay archivos para procesar", "error")
        with state_lock:
            state["is_running"] = False
        return

    with state_lock:
        state["total_files"] = len(audio_files)

    add_log(user_id, f"Encontrados {len(audio_files)} archivos")

    merge_mode = config.get("unir_reunion", False)

    if merge_mode and len(audio_files) > 1:
        # === MODO REUNION UNIFICADA ===
        add_log(user_id, f"Modo reunion unificada: {len(audio_files)} archivos")
        all_names = [f["name"] for f in audio_files]
        all_transcriptions = []
        total_duration = 0
        previous_context = ""
        accumulated_offset = 0

        try:
            for i, file_info in enumerate(audio_files):
                with state_lock:
                    if state["should_stop"]:
                        add_log(user_id, "Procesamiento detenido por el usuario")
                        raise Exception("Detenido por el usuario")
                    state["current_file"] = file_info["name"]
                    state["progress"] = int((i / len(audio_files)) * 100)

                file_path = user_dir / file_info["name"]
                add_log(user_id, f"Parte [{i+1}/{len(audio_files)}]: {file_info['name']}")

                mp3_path = convert_to_mp3(file_path)
                if mp3_path is None:
                    raise Exception(f"No se pudo convertir {file_info['name']}")

                file_duration = get_audio_duration(mp3_path)
                total_duration += file_duration

                # Transcribir cada archivo (puede dividirse internamente si es largo)
                # Para archivos individuales, transcribimos directo con contexto previo
                duration = get_audio_duration(mp3_path)
                segment_duration = 480

                if duration <= segment_duration:
                    text = transcribe_with_gemini(
                        mp3_path, config, user_id,
                        previous_context=previous_context if i > 0 else ""
                    )
                else:
                    # Archivo largo: dividir internamente
                    segments = split_audio(mp3_path, segment_duration)
                    if not segments:
                        raise Exception(f"No se pudieron crear segmentos de {file_info['name']}")

                    part_texts = []
                    for j, seg_path in enumerate(segments):
                        seg_offset = j * segment_duration
                        add_log(user_id, f"  Sub-parte {j+1}/{len(segments)}...")

                        seg_text = transcribe_with_gemini(
                            seg_path, config, user_id,
                            previous_context=previous_context if (i > 0 or j > 0) else ""
                        )

                        if i == 0 and j == 0:
                            previous_context = extract_speaker_context(seg_text, config)

                        if seg_offset > 0:
                            seg_text = adjust_timestamps(seg_text, seg_offset)

                        part_texts.append(seg_text)

                    cleanup_segments(segments, mp3_path)
                    text = "\n".join(part_texts)

                # Actualizar contexto de hablantes (solo la primera vez)
                if i == 0 and not previous_context:
                    previous_context = extract_speaker_context(text, config)

                # Ajustar timestamps con offset acumulado
                if accumulated_offset > 0:
                    text = adjust_timestamps(text, accumulated_offset)

                all_transcriptions.append(f"--- {file_info['name']} ---\n{text}")
                accumulated_offset += int(file_duration)

                # Borrar audio procesado
                try:
                    os.remove(str(file_path))
                    if mp3_path != str(file_path) and Path(mp3_path).exists():
                        os.remove(mp3_path)
                except:
                    pass

                with state_lock:
                    state["processed_files"] += 1

        except Exception as e:
            add_log(user_id, f"Error en reunion unificada: {e}", "error")
            logging.exception(e)
            with state_lock:
                state["is_running"] = False
                state["current_file"] = None
            return

        # Guardar transcripcion unificada
        merged_text = clean_transcription("\n\n".join(all_transcriptions))
        merged_name = " + ".join(all_names)
        duration_mins = int(total_duration / 60)

        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO transcripciones (usuario_id, archivo, transcripcion, tipo_audio, duracion_minutos)
                       VALUES (%s, %s, %s, %s, %s)""",
                    (user_id, merged_name, merged_text,
                     config.get("tipo_audio", "telemarketing"), duration_mins)
                )
            conn.commit()
            add_log(user_id, f"Reunion unificada guardada: {len(audio_files)} partes, {duration_mins} min")
        finally:
            conn.close()

        with state_lock:
            state["last_transcription"] = merged_text

    else:
        # === MODO INDIVIDUAL (cada archivo por separado) ===
        for i, file_info in enumerate(audio_files):
            with state_lock:
                if state["should_stop"]:
                    add_log(user_id, "Procesamiento detenido por el usuario")
                    break
                state["current_file"] = file_info["name"]
                state["progress"] = int((i / len(audio_files)) * 100)

            file_path = user_dir / file_info["name"]
            add_log(user_id, f"Procesando [{i+1}/{len(audio_files)}]: {file_info['name']}")

            try:
                mp3_path = convert_to_mp3(file_path)
                if mp3_path is None:
                    add_log(user_id, f"No se pudo convertir {file_info['name']}", "error")
                    continue

                add_log(user_id, "Transcribiendo con Gemini...")
                transcription_text, duration_mins = transcribe_long_audio(mp3_path, config, user_id)
                cleaned_text = clean_transcription(transcription_text)
                add_log(user_id, "Transcripcion completada")

                conn = get_db()
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            """INSERT INTO transcripciones (usuario_id, archivo, transcripcion, tipo_audio, duracion_minutos)
                               VALUES (%s, %s, %s, %s, %s)""",
                            (user_id, file_info["name"], cleaned_text,
                             config.get("tipo_audio", "telemarketing"), duration_mins)
                        )
                    conn.commit()
                    add_log(user_id, f"Guardado en base de datos: {file_info['name']}")
                finally:
                    conn.close()

                try:
                    os.remove(str(file_path))
                    if mp3_path != str(file_path) and Path(mp3_path).exists():
                        os.remove(mp3_path)
                    add_log(user_id, f"Audio eliminado: {file_info['name']}")
                except Exception as e:
                    logging.warning(f"No se pudo borrar audio: {e}")

                with state_lock:
                    state["processed_files"] += 1
                    state["last_transcription"] = cleaned_text

            except Exception as e:
                add_log(user_id, f"Error procesando {file_info['name']}: {e}", "error")
                logging.exception(e)

    # Limpiar carpeta del usuario si esta vacia
    try:
        if user_dir.exists() and not list(user_dir.iterdir()):
            user_dir.rmdir()
    except:
        pass

    with state_lock:
        state["is_running"] = False
        state["current_file"] = None
        state["progress"] = 100

    add_log(user_id, f"Completado: {state['processed_files']}/{len(audio_files)} archivos")


# ============ RUTAS DE LA API ============

@app.route('/')
@login_required
def index():
    return render_template('index.html', nombre=session.get('nombre', session.get('username')))


@app.route('/api/status')
@login_required
def get_status():
    user_id = session['user_id']
    state = get_user_state(user_id)
    with state_lock:
        return jsonify({
            "is_running": state["is_running"],
            "current_file": state["current_file"],
            "progress": state["progress"],
            "total_files": state["total_files"],
            "processed_files": state["processed_files"],
            "logs": state["logs"][-20:],
            "last_transcription": state["last_transcription"]
        })


@app.route('/api/files')
@login_required
def api_get_files():
    files = get_audio_files(session['user_id'])
    return jsonify({"files": files, "total": len(files)})


# ---- Chunked upload ----

@app.route('/api/upload/start', methods=['POST'])
@login_required
def upload_start():
    data = request.json or {}
    filename = Path(data.get('filename', 'audio.mp3')).name
    total_chunks = data.get('total_chunks', 1)

    upload_id = secrets.token_hex(8)
    temp_dir = BASE_DIR / "temp_uploads" / upload_id
    temp_dir.mkdir(parents=True, exist_ok=True)

    meta = {"filename": filename, "total_chunks": total_chunks, "received": 0,
            "user_id": session['user_id']}
    with open(temp_dir / "meta.json", 'w') as f:
        json.dump(meta, f)

    return jsonify({"upload_id": upload_id})


@app.route('/api/upload/chunk', methods=['POST'])
@login_required
def upload_chunk():
    upload_id = request.form.get('upload_id')
    chunk_index = int(request.form.get('chunk_index', 0))

    if not upload_id or 'chunk' not in request.files:
        return jsonify({"error": "Datos incompletos"}), 400

    temp_dir = BASE_DIR / "temp_uploads" / upload_id
    if not temp_dir.exists():
        return jsonify({"error": "Upload no encontrado"}), 404

    chunk = request.files['chunk']
    chunk.save(str(temp_dir / f"chunk_{chunk_index:04d}"))

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
    data = request.json or {}
    upload_id = data.get('upload_id')

    if not upload_id:
        return jsonify({"error": "upload_id requerido"}), 400

    temp_dir = BASE_DIR / "temp_uploads" / upload_id
    if not temp_dir.exists():
        return jsonify({"error": "Upload no encontrado"}), 404

    with open(temp_dir / "meta.json") as f:
        meta = json.load(f)

    user_id = session['user_id']
    filename = meta["filename"]

    # Carpeta por usuario
    user_dir = INPUT_FOLDER / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    dest = user_dir / filename

    with open(dest, 'wb') as out:
        for i in range(meta["total_chunks"]):
            chunk_path = temp_dir / f"chunk_{i:04d}"
            if not chunk_path.exists():
                return jsonify({"error": f"Chunk {i} faltante"}), 400
            with open(chunk_path, 'rb') as inp:
                out.write(inp.read())

    shutil.rmtree(str(temp_dir), ignore_errors=True)

    add_log(user_id, f"Archivo subido: {filename} ({dest.stat().st_size / (1024*1024):.1f} MB)")

    return jsonify({"filename": filename, "size_mb": round(dest.stat().st_size / (1024*1024), 2)})


# ---- Procesamiento ----

@app.route('/api/start', methods=['POST'])
@login_required
def start_processing():
    user_id = session['user_id']
    state = get_user_state(user_id)

    with state_lock:
        if state["is_running"]:
            return jsonify({"error": "Ya hay un procesamiento en curso"}), 400

    config = request.json or {}

    thread = threading.Thread(target=process_files, args=(config, user_id))
    thread.daemon = True
    thread.start()

    return jsonify({"message": "Procesamiento iniciado"})


@app.route('/api/stop', methods=['POST'])
@login_required
def stop_processing():
    state = get_user_state(session['user_id'])
    with state_lock:
        state["should_stop"] = True
    return jsonify({"message": "Deteniendo..."})


# ---- Historial ----

@app.route('/api/historial')
@login_required
def get_historial():
    user_id = session['user_id']
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT id, archivo, tipo_audio, duracion_minutos, fecha
                   FROM transcripciones WHERE usuario_id = %s
                   ORDER BY fecha DESC LIMIT 100""",
                (user_id,)
            )
            rows = cur.fetchall()
            for r in rows:
                r['fecha'] = r['fecha'].strftime('%Y-%m-%d %H:%M')
        return jsonify({"historial": rows})
    finally:
        conn.close()


@app.route('/api/transcripcion/<int:trans_id>')
@login_required
def get_transcripcion(trans_id):
    user_id = session['user_id']
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, archivo, transcripcion, tipo_audio, duracion_minutos, fecha FROM transcripciones WHERE id = %s AND usuario_id = %s",
                (trans_id, user_id)
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "No encontrada"}), 404
            row['fecha'] = row['fecha'].strftime('%Y-%m-%d %H:%M')
            return jsonify(row)
    finally:
        conn.close()


@app.route('/api/transcripcion/<int:trans_id>', methods=['DELETE'])
@login_required
def delete_transcripcion(trans_id):
    user_id = session['user_id']
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM transcripciones WHERE id = %s AND usuario_id = %s",
                (trans_id, user_id)
            )
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


# ============ INICIO ============

# Inicializar DB al arrancar
try:
    init_db()
except Exception as e:
    logging.error(f"No se pudo conectar a la DB: {e}")

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5050))
    print(f"Servidor en http://0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
