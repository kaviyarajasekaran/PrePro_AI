import os
import sqlite3
from datetime import datetime
from functools import wraps

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from authlib.integrations.flask_client import OAuth
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    flash,
    send_from_directory,
    jsonify,
)
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

from dotenv import load_dotenv
load_dotenv()

# -----------------------------
# App config
# -----------------------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
PROCESSED_FOLDER = os.path.join(BASE_DIR, "processed")
VISUALIZATION_FOLDER = os.path.join(BASE_DIR, "visualizations")
DB_PATH = os.path.join(BASE_DIR, "preproai.db")

ALLOWED_EXTENSIONS = {"csv", "xlsx", "xls"}

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)
os.makedirs(VISUALIZATION_FOLDER, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "preproai_secret_key")

oauth = OAuth(app)

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")

oauth.register(
    name="google",
    client_id=GOOGLE_CLIENT_ID,
    client_secret=GOOGLE_CLIENT_SECRET,
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)

# -----------------------------
# Google OAuth
# -----------------------------
@app.route("/auth/google")
def google_login():
    redirect_uri = url_for("google_callback", _external=True)
    return oauth.google.authorize_redirect(redirect_uri)

@app.route("/auth/google/callback")
def google_callback():
    try:
        token = oauth.google.authorize_access_token()
    except Exception:
        flash("Google login failed. Try again.", "error")
        return redirect(url_for("login"))

    userinfo = token.get("userinfo")
    if not userinfo:
        try:
            userinfo = oauth.google.parse_id_token(token)
        except Exception:
            userinfo = None

    email = ((userinfo or {}).get("email") or "").lower().strip()
    if not email:
        flash("Google login failed: email not found", "error")
        return redirect(url_for("login"))

    # create user if not exists
    with get_db() as conn:
        user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if not user:
            conn.execute(
                "INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)",
                (email, generate_password_hash(os.urandom(16).hex()), datetime.utcnow().isoformat()),
            )
            conn.commit()
            user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()

    session["user_id"] = int(user["id"])
    session["user_email"] = email
    session["user"] = email
    return redirect(url_for("dashboard", tab="tabUpload"))

# -----------------------------
# DB SAFE CONNECTION
# -----------------------------
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn

def init_db():
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                filename TEXT NOT NULL,
                stored_filename TEXT NOT NULL,
                uploaded_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
            """
        )
        conn.commit()
init_db()

# -----------------------------
# Auth Helpers
# -----------------------------
def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)
    return wrapper

# -----------------------------
# Utilities
# -----------------------------
def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def read_dataframe(file_path: str) -> pd.DataFrame:
    ext = file_path.rsplit(".", 1)[-1].lower()
    if ext == "csv":
        return pd.read_csv(file_path)
    if ext in ("xlsx", "xls"):
        return pd.read_excel(file_path)
    raise ValueError("Unsupported file format")

def save_dataframe_as_csv(df: pd.DataFrame, out_path: str):
    df.to_csv(out_path, index=False)

def list_user_uploaded_files(user_id: int):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT filename, stored_filename, uploaded_at FROM user_files WHERE user_id = ? ORDER BY id DESC",
            (user_id,),
        ).fetchall()
    return [dict(r) for r in rows]

def list_user_processed_files(user_id: int):
    files = []
    prefix = f"{user_id}__"
    for f in os.listdir(PROCESSED_FOLDER):
        if f.startswith(prefix):
            files.append(f)
    return sorted(files)

def owns_file(user_id: int, stored_filename: str) -> bool:
    with get_db() as conn:
        row = conn.execute(
            "SELECT 1 FROM user_files WHERE user_id = ? AND stored_filename = ?",
            (user_id, stored_filename),
        ).fetchone()
    return row is not None

# -----------------------------
# Routes: Public
# -----------------------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/about")
def about():
    return render_template("about.html")

# -----------------------------
# Routes: Auth
# -----------------------------
@app.route("/login", methods=["GET", "POST"])
def login():

    # NEW FIX ⭐
    if session.get("user_id"):
        return redirect(url_for("dashboard", tab="tabUpload"))

    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = (request.form.get("password") or "").strip()

        if not email or not password:
            flash("Please enter email and password.", "auth")
            return redirect(url_for("login"))

        with get_db() as conn:
            user = conn.execute(
                "SELECT * FROM users WHERE email = ?",
                (email,)
            ).fetchone()

        if not user or not check_password_hash(user["password_hash"], password):
            flash("Invalid email or password.", "auth")
            return redirect(url_for("login"))

        session["user_id"] = int(user["id"])
        session["user_email"] = user["email"]
        session["user"] = user["email"]

        return redirect(url_for("dashboard", tab="tabUpload"))

    return render_template("login.html")

@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = (request.form.get("password") or "").strip()

        if not email or not password:
            flash("Please enter email and password.", "error")
            return redirect(url_for("signup"))

        password_hash = generate_password_hash(password)

        try:
            with get_db() as conn:
                conn.execute(
                    "INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)",
                    (email, password_hash, datetime.utcnow().isoformat()),
                )
                conn.commit()
        except sqlite3.IntegrityError:
            flash("This email is already registered. Please login.", "error")
            return redirect(url_for("login"))

        flash("Account created! Please login.", "success")
        return redirect(url_for("login"))

    return render_template("signup.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("index"))

# -----------------------------
# Routes: Dashboard
# -----------------------------
@app.route("/dashboard")
@login_required
def dashboard():
    user_id = session["user_id"]
    user_files = list_user_uploaded_files(user_id)
    processed_files = list_user_processed_files(user_id)

    active_tab = request.args.get("tab") or "tabUpload"

    return render_template(
        "dashboard.html",
        user_files=user_files,
        processed_files=processed_files,
        user_email=session.get("user_email"),
        active_tab=active_tab,
    )

# -----------------------------
# Upload (multiple)
# -----------------------------
@app.route("/upload", methods=["POST"])
@login_required
def upload():
    if "files" not in request.files:
        flash("No file part found.", "error")
        return redirect(url_for("dashboard", tab="tabUpload"))

    user_id = session["user_id"]
    files = request.files.getlist("files")

    saved = 0
    rejected = 0

    for file in files:
        if not file or not file.filename:
            continue

        if not allowed_file(file.filename):
            rejected += 1
            continue

        original = secure_filename(file.filename)
        stored = f"{user_id}__{original}"
        out_path = os.path.join(UPLOAD_FOLDER, stored)
        file.save(out_path)

        with get_db() as conn:
            conn.execute(
                "INSERT INTO user_files (user_id, filename, stored_filename, uploaded_at) VALUES (?, ?, ?, ?)",
                (user_id, original, stored, datetime.utcnow().isoformat()),
            )
            conn.commit()

        saved += 1

    if saved:
        flash(f"Uploaded {saved} file(s).", "success")
    if rejected:
        flash(f"Rejected {rejected} file(s). Only CSV/XLSX/XLS allowed.", "error")

    return redirect(url_for("dashboard", tab="tabUpload"))

# -----------------------------
# Preview (top rows)
# -----------------------------
@app.route("/preview")
@login_required
def preview():
    user_id = session["user_id"]
    stored = (request.args.get("file") or "").strip()

    if not stored:
        return jsonify({"error": "file parameter is required"}), 400

    if not owns_file(user_id, stored) and not stored.startswith(f"{user_id}__"):
        return jsonify({"error": "Unauthorized file"}), 403

    path1 = os.path.join(UPLOAD_FOLDER, stored)
    path2 = os.path.join(PROCESSED_FOLDER, stored)

    file_path = path1 if os.path.exists(path1) else path2
    if not os.path.exists(file_path):
        return jsonify({"error": "File not found"}), 404

    try:
        df = read_dataframe(file_path).head(25)
    except Exception as e:
        return jsonify({"error": f"Cannot read: {e}"}), 400

    return jsonify({"columns": df.columns.tolist(), "rows": df.fillna("").astype(str).values.tolist()})

# -----------------------------
# Clean (options)
# -----------------------------
@app.route("/clean", methods=["POST"])
@login_required
def clean():
    user_id = session["user_id"]
    selected = request.form.getlist("selected_files")

    do_drop_duplicates = request.form.get("opt_duplicates") == "on"
    do_fill_missing = request.form.get("opt_missing") == "on"
    do_sort = request.form.get("opt_sort") == "on"
    sort_col = (request.form.get("sort_col") or "").strip()

    if not selected:
        flash("Please select at least one file to clean.", "error")
        return redirect(url_for("dashboard", tab="tabClean"))

    cleaned_count = 0
    for stored in selected:
        if not owns_file(user_id, stored):
            continue

        src = os.path.join(UPLOAD_FOLDER, stored)
        if not os.path.exists(src):
            continue

        try:
            df = read_dataframe(src)
        except Exception:
            continue

        if do_drop_duplicates:
            df = df.drop_duplicates()

        if do_fill_missing:
            num_cols = df.select_dtypes(include="number").columns
            if len(num_cols) > 0:
                df[num_cols] = df[num_cols].fillna(df[num_cols].mean())
            df = df.fillna("")

        if do_sort and sort_col and sort_col in df.columns:
            df = df.sort_values(by=sort_col)

        original_display = stored.split("__", 1)[-1]
        out_name = f"{user_id}__cleaned_{os.path.splitext(original_display)[0]}.csv"
        out_path = os.path.join(PROCESSED_FOLDER, out_name)
        save_dataframe_as_csv(df, out_path)

        cleaned_count += 1

    if cleaned_count:
        flash(f"Cleaned {cleaned_count} file(s).", "success")
    else:
        flash("No files cleaned. Check selected files.", "error")

    return redirect(url_for("dashboard", tab="tabClean"))

# -----------------------------
# Summary
# -----------------------------
@app.route("/summary", methods=["POST"])
@login_required
def summary():
    user_id = session["user_id"]
    stored = (request.form.get("summary_file") or "").strip()

    if not stored:
        flash("Please select a file for summary.", "error")
        return redirect(url_for("dashboard", tab="tabSummary"))

    path_up = os.path.join(UPLOAD_FOLDER, stored)
    path_pr = os.path.join(PROCESSED_FOLDER, stored)

    file_path = path_up if os.path.exists(path_up) else path_pr
    if not os.path.exists(file_path):
        flash("Selected file not found.", "error")
        return redirect(url_for("dashboard", tab="tabSummary"))

    if file_path == path_up and not owns_file(user_id, stored):
        flash("Unauthorized file.", "error")
        return redirect(url_for("dashboard", tab="tabSummary"))
    if file_path == path_pr and not stored.startswith(f"{user_id}__"):
        flash("Unauthorized file.", "error")
        return redirect(url_for("dashboard", tab="tabSummary"))

    try:
        df = read_dataframe(file_path)
    except Exception:
        flash("Unable to read the selected file.", "error")
        return redirect(url_for("dashboard", tab="tabSummary"))

    info = {
        "rows": int(df.shape[0]),
        "cols": int(df.shape[1]),
        "columns": list(df.columns),
        "missing": int(df.isna().sum().sum()),
        "duplicates": int(df.duplicated().sum()),
    }

    user_files = list_user_uploaded_files(user_id)
    processed_files = list_user_processed_files(user_id)

    return render_template(
        "dashboard.html",
        user_files=user_files,
        processed_files=processed_files,
        summary_info=info,
        summary_filename=stored.split("__", 1)[-1],
        user_email=session.get("user_email"),
        active_tab="tabSummary",
    )

# -----------------------------
# Visualize
# -----------------------------
@app.route("/visualizations/<path:filename>")
@login_required
def viz_file(filename):
    return send_from_directory(VISUALIZATION_FOLDER, filename)

@app.route("/visualize", methods=["POST"], endpoint="visualize")
@login_required
def visualize():
    user_id = session["user_id"]
    stored = (request.form.get("viz_file") or "").strip()
    chart_type = (request.form.get("chart_type") or "histogram").strip().lower()

    if not stored:
        flash("Please select a file to visualize.", "error")
        return redirect(url_for("dashboard", tab="tabViz"))

    path_up = os.path.join(UPLOAD_FOLDER, stored)
    path_pr = os.path.join(PROCESSED_FOLDER, stored)
    file_path = path_up if os.path.exists(path_up) else path_pr

    if not os.path.exists(file_path):
        flash("Selected file not found.", "error")
        return redirect(url_for("dashboard", tab="tabViz"))

    if file_path == path_up and not owns_file(user_id, stored):
        flash("Unauthorized file.", "error")
        return redirect(url_for("dashboard", tab="tabViz"))
    if file_path == path_pr and not stored.startswith(f"{user_id}__"):
        flash("Unauthorized file.", "error")
        return redirect(url_for("dashboard", tab="tabViz"))

    try:
        df = read_dataframe(file_path)
    except Exception as e:
        flash(f"Cannot read: {e}", "error")
        return redirect(url_for("dashboard", tab="tabViz"))

    plt.figure(figsize=(9, 5))

    try:
        if chart_type == "histogram":
            df.select_dtypes(include="number").hist()
        elif chart_type == "line":
            df.select_dtypes(include="number").plot(kind="line")
        elif chart_type == "bar":
            df.iloc[:, 0].value_counts().plot(kind="bar")
        elif chart_type == "pie":
            df.iloc[:, 0].value_counts().plot(kind="pie", autopct="%1.1f%%")
        elif chart_type == "scatter":
            if df.shape[1] < 2:
                raise ValueError("Need at least 2 columns")
            sns.scatterplot(x=df.iloc[:, 0], y=df.iloc[:, 1])
        else:
            raise ValueError("Invalid chart type")
    except Exception as e:
        flash(f"Visualization failed: {e}", "error")
        return redirect(url_for("dashboard", tab="tabViz"))
    finally:
        plt.tight_layout()

    out_name = f"{user_id}__viz_{os.path.splitext(stored.split('__',1)[-1])[0]}_{chart_type}.png"
    out_path = os.path.join(VISUALIZATION_FOLDER, out_name)
    plt.savefig(out_path)
    plt.close()

    chart_url = url_for("viz_file", filename=out_name)

    user_files = list_user_uploaded_files(user_id)
    processed_files = list_user_processed_files(user_id)

    return render_template(
        "dashboard.html",
        user_files=user_files,
        processed_files=processed_files,
        chart_url=chart_url,
        user_email=session.get("user_email"),
        active_tab="tabViz",
    )

# -----------------------------
# Download processed
# -----------------------------
@app.route("/download/<path:filename>")
@login_required
def download_processed(filename):
    return send_from_directory(PROCESSED_FOLDER, filename, as_attachment=True)

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    init_db()
    app.run(debug=True, use_reloader=False)
