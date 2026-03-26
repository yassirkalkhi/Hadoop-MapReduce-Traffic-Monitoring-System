"""
Application Flask — Système de Surveillance du Trafic (STMS)
Point d'entrée principal de l'API REST et du tableau de bord.
"""

from flask import Flask
from flask_cors import CORS

from routes.jobs   import jobs_bp
from routes.data   import data_bp
from routes.status import status_bp

# ─── Initialisation de l'application ─────────────────────────────────────────
app = Flask(__name__, template_folder="templates", static_folder="static")
CORS(app)   # Autorise les requêtes cross-origin pour les appels AJAX

# ─── Enregistrement des blueprints ───────────────────────────────────────────
app.register_blueprint(jobs_bp,   url_prefix="/run")
app.register_blueprint(data_bp,   url_prefix="/data")
app.register_blueprint(status_bp, url_prefix="/status")

# ─── Route principale : tableau de bord ──────────────────────────────────────
from flask import render_template

@app.route("/")
def dashboard():
    """Rendu de la page principale du tableau de bord."""
    return render_template("index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
