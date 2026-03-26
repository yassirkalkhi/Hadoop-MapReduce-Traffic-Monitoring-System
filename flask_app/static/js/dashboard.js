/**
 * STMS — Logique principale du tableau de bord
 * Chargement des données, rendu des graphiques, déclenchement des travaux.
 */

/* ─── État global de l'application ─────────────────────────────────────── */
const state = {
  charts:   {},          // Instances Chart.js stockées par nom
  jobStatus: {},         // Statut de disponibilité de chaque travail
  loading:   new Set(),  // Travaux en cours de chargement/exécution
};

/* ─── Configuration des couleurs Chart.js ───────────────────────────────── */
const CHART_COLORS = {
  blue:    { solid: '#3b82f6', alpha: 'rgba(59,130,246,0.15)' },
  purple:  { solid: '#6366f1', alpha: 'rgba(99,102,241,0.15)' },
  green:   { solid: '#10b981', alpha: 'rgba(16,185,129,0.15)' },
  amber:   { solid: '#f59e0b', alpha: 'rgba(245,158,11,0.15)' },
  red:     { solid: '#ef4444', alpha: 'rgba(239,68,68,0.15)'  },
};

/* ─── Initialisation globale Chart.js ───────────────────────────────────── */
Chart.defaults.color           = '#8fa3c8';
Chart.defaults.borderColor     = 'rgba(255,255,255,0.06)';
Chart.defaults.font.family     = "'Inter', system-ui, sans-serif";
Chart.defaults.plugins.legend.display = false;

/* ========================================================================
   UTILITAIRES
   ======================================================================== */

/**
 * Formate un nombre avec séparateur de milliers.
 * @param {number} n
 * @returns {string}
 */
function fmt(n) {
  return typeof n === 'number' ? n.toLocaleString('fr-FR') : n;
}

/**
 * Affiche une notification toast.
 * @param {'success'|'error'|'info'|'warning'} type
 * @param {string} title
 * @param {string} [msg]
 */
function showToast(type, title, msg = '') {
  const icons = { 
    success: "<i class='bx bx-check-circle'></i>", 
    error: "<i class='bx bx-error-circle'></i>", 
    info: "<i class='bx bx-info-circle'></i>", 
    warning: "<i class='bx bx-error'></i>" 
  };
  const container = document.getElementById('toast-container');

  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `
    <span class="toast-icon">${icons[type]}</span>
    <div class="toast-text">
      <div class="toast-title">${title}</div>
      ${msg ? `<div class="toast-msg">${msg}</div>` : ''}
    </div>
  `;
  container.appendChild(toast);

  // Suppression automatique après 4 secondes
  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateX(20px)';
    toast.style.transition = 'all 0.3s ease';
    setTimeout(() => toast.remove(), 300);
  }, 4000);
}

/**
 * Met à jour l'affichage d'un indicateur de statut de travail.
 * @param {string} jobKey
 * @param {'ready'|'missing'|'running'|'error'} status
 * @param {string} [label]
 */
function setJobIndicator(jobKey, status, label = '') {
  const el = document.getElementById(`status-${jobKey}`);
  if (!el) return;

  const labels = {
    ready:   'Résultats disponibles',
    missing: 'En attente d\'exécution',
    running: 'En cours d\'exécution...',
    error:   'Erreur lors de l\'exécution',
  };

  el.innerHTML = `
    <span class="job-dot ${status}"></span>
    <span style="color:var(--color-text-muted)">${label || labels[status]}</span>
  `;
}

/**
 * Affiche ou masque un loader par-dessus un conteneur parent (carte, graphe, table)
 * @param {string} elementId ID de l'élément enfant
 * @param {boolean} isLoading True pour afficher le loader, False pour le masquer
 */
function toggleLoading(elementId, isLoading) {
  const el = document.getElementById(elementId);
  if (!el) return;
  const parent = el.closest('.chart-card') || el.closest('.stat-card') || el.closest('.table-section');
  if (!parent) return;
  
  if (isLoading) {
    if (!parent.querySelector('.loading-overlay')) {
      // Ensure the parent is pos relative so the absolute overlay is contained
      if (window.getComputedStyle(parent).position === 'static') {
        parent.style.position = 'relative';
      }
      const overlay = document.createElement('div');
      overlay.className = 'loading-overlay';
      overlay.innerHTML = '<div class="spinner"></div>';
      parent.appendChild(overlay);
    }
  } else {
    const overlay = parent.querySelector('.loading-overlay');
    if (overlay) overlay.remove();
  }
}

/* ========================================================================
   CHARGEMENT DU STATUT
   ======================================================================== */

/**
 * Vérifie la disponibilité des résultats MapReduce dans HDFS.
 */
async function loadStatus() {
  try {
    const r = await fetch('/status');
    const d = await r.json();
    if (d.success) {
      state.jobStatus = d.jobs;
      Object.entries(d.jobs).forEach(([job, ready]) => {
        setJobIndicator(job, ready ? 'ready' : 'missing');
      });
    }
  } catch (_) {
    // Silence : le serveur n'est peut-être pas encore prêt
  }
}

/* ========================================================================
   DÉCLENCHEMENT DES TRAVAUX MAPREDUCE
   ======================================================================== */

/**
 * Lance un travail MapReduce via l'API Flask.
 * @param {string} jobKey  - ex : 'vehicle_count', 'all'
 */
async function runJob(jobKey) {
  if (state.loading.has(jobKey)) return;  // Évite les doubles soumissions

  state.loading.add(jobKey);
  const btn = document.getElementById(`btn-run-${jobKey}`);
  if (btn) {
    btn.classList.add('btn-loading');
    btn.querySelector('.btn-icon').textContent = '⏳';
  }

  setJobIndicator(jobKey, 'running');
  showToast('info', 'Travail lancé', `Exécution de "${jobKey}" en cours...`);

  try {
    const r = await fetch(`/run/${jobKey}`, { method: 'POST' });
    const d = await r.json();

    if (d.success) {
      setJobIndicator(jobKey, 'ready');
      showToast('success', 'Travail terminé', `"${jobKey}" exécuté avec succès.`);
      // Recharge les données correspondantes
      if (jobKey !== 'all') {
        await loadJobData(jobKey);
      } else {
        await loadAllData();
      }
    } else {
      setJobIndicator(jobKey, 'error');
      showToast('error', 'Échec du travail', d.error || 'Erreur inconnue.');
    }
  } catch (err) {
    setJobIndicator(jobKey, 'error');
    showToast('error', 'Erreur réseau', err.message);
  } finally {
    state.loading.delete(jobKey);
    if (btn) {
      btn.classList.remove('btn-loading');
      btn.querySelector('.btn-icon').textContent = '▶';
    }
  }
}

/* ========================================================================
   CHARGEMENT ET RENDU DES DONNÉES
   ======================================================================== */

/**
 * Charge les données d'un travail depuis l'API.
 * @param {string} jobKey
 * @returns {Promise<Object|null>}
 */
async function fetchJobData(jobKey) {
  const r = await fetch(`/data/${jobKey}`);
  if (!r.ok) return null;
  return r.json();
}

/**
 * Charge et affiche les données du travail 1 : comptage de véhicules.
 */
async function loadVehicleCount() {
  toggleLoading('chart-vehicle-count', true);
  toggleLoading('kpi-total', true);
  toggleLoading('table-vehicle-count', true);

  const d = await fetchJobData('vehicle_count');

  toggleLoading('chart-vehicle-count', false);
  toggleLoading('kpi-total', false);
  toggleLoading('table-vehicle-count', false);

  if (!d || !d.success) return;

  const data   = d.data;
  const labels = data.map(x => x.road_id);
  const values = data.map(x => x.value);

  // Mise à jour de la carte statistique : total des véhicules
  const total = values.reduce((a, b) => a + b, 0);
  document.getElementById('stat-total-vehicles').textContent = fmt(total);

  // Graphique à barres
  renderBarChart('chart-vehicle-count', labels, values, 'Véhicules', CHART_COLORS.blue);

  // Table des routes les plus chargées (top 10)
  renderVehicleTable(data.slice(0, 10));
}

/**
 * Charge et affiche les données du travail 2 : vitesse moyenne.
 */
async function loadAvgSpeed() {
  toggleLoading('chart-avg-speed', true);
  toggleLoading('kpi-speed', true);
  toggleLoading('table-avg-speed', true);

  const d = await fetchJobData('avg_speed');

  toggleLoading('chart-avg-speed', false);
  toggleLoading('kpi-speed', false);
  toggleLoading('table-avg-speed', false);

  if (!d || !d.success) return;

  const data   = d.data;
  const labels = data.map(x => x.road_id);
  const values = data.map(x => x.value);

  // Mise à jour de la carte statistique : vitesse globale moyenne
  const global  = values.reduce((a, b) => a + b, 0) / values.length;
  document.getElementById('stat-avg-speed').textContent = `${global.toFixed(1)} km/h`;

  // Graphique en ligne
  renderLineChart('chart-avg-speed', labels, values, 'Vitesse moy. (km/h)', CHART_COLORS.purple);

  // Table des routes les plus lentes (déjà tri croissant depuis l'API)
  renderSpeedTable(data.slice(0, 10));
}

/**
 * Charge et affiche les données du travail 3 : heures de pointe.
 */
async function loadPeakHours() {
  toggleLoading('chart-peak-hours', true);
  
  const d = await fetchJobData('peak_hours');
  
  toggleLoading('chart-peak-hours', false);

  if (!d || !d.success) return;

  const data   = d.data;
  const labels = data.map(x => `${String(x.hour).padStart(2, '0')}h`);
  const values = data.map(x => x.value);

  renderAreaChart('chart-peak-hours', labels, values, 'Passages', CHART_COLORS.amber);
}

/**
 * Charge et affiche les données du travail 4 : congestion.
 */
async function loadCongestion() {
  toggleLoading('kpi-congested', true);
  toggleLoading('table-congestion', true);
  
  const d = await fetchJobData('congestion');
  
  toggleLoading('kpi-congested', false);
  toggleLoading('table-congestion', false);

  if (!d || !d.success) return;

  const data       = d.data;
  const congested  = data.filter(x => x.congested);

  // Mise à jour de la carte statistique : route la plus congestionnée
  if (congested.length > 0) {
    document.getElementById('stat-congested-road').textContent = congested[0].road_id;
    document.getElementById('stat-congested-count').textContent =
      `${congested.length} route${congested.length > 1 ? 's' : ''} congestionnée${congested.length > 1 ? 's' : ''}`;
  }

  // Table des routes congestionnées
  renderCongestionTable(data.slice(0, 15));
}

/**
 * Charge toutes les données disponibles.
 */
async function loadAllData() {
  await Promise.all([
    loadVehicleCount(),
    loadAvgSpeed(),
    loadPeakHours(),
    loadCongestion(),
  ]);
}

/**
 * Charge les données d'un seul travail par nom de clé.
 * @param {string} jobKey
 */
async function loadJobData(jobKey) {
  const loaders = {
    vehicle_count: loadVehicleCount,
    avg_speed:     loadAvgSpeed,
    peak_hours:    loadPeakHours,
    congestion:    loadCongestion,
  };
  if (loaders[jobKey]) await loaders[jobKey]();
}

/* ========================================================================
   RENDU DES GRAPHIQUES
   ======================================================================== */

/**
 * Configure les options de base partagées par tous les graphiques.
 */
function baseOptions(xGrid = false) {
  return {
    responsive:          true,
    maintainAspectRatio: false,
    animation:           { duration: 600, easing: 'easeOutQuart' },
    plugins: {
      tooltip: {
        backgroundColor: '#1a2235',
        borderColor:     'rgba(255,255,255,0.08)',
        borderWidth:     1,
        titleColor:      '#f0f4ff',
        bodyColor:       '#8fa3c8',
        padding:         10,
        cornerRadius:    8,
      },
    },
    scales: {
      x: {
        grid:  { display: xGrid, color: 'rgba(255,255,255,0.04)' },
        ticks: { color: '#4a5a78', font: { size: 11 }, maxRotation: 45 },
      },
      y: {
        grid:  { color: 'rgba(255,255,255,0.04)' },
        ticks: { color: '#4a5a78', font: { size: 11 } },
      },
    },
  };
}

/**
 * Crée ou met à jour un graphique en barres.
 */
function renderBarChart(canvasId, labels, data, label, color) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return;

  if (state.charts[canvasId]) state.charts[canvasId].destroy();

  state.charts[canvasId] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label,
        data,
        backgroundColor: color.alpha,
        borderColor:     color.solid,
        borderWidth:     1.5,
        borderRadius:    5,
        borderSkipped:   false,
      }],
    },
    options: {
      ...baseOptions(),
      plugins: { ...baseOptions().plugins },
    },
  });
}

/**
 * Crée ou met à jour un graphique en ligne.
 */
function renderLineChart(canvasId, labels, data, label, color) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return;

  if (state.charts[canvasId]) state.charts[canvasId].destroy();

  state.charts[canvasId] = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label,
        data,
        borderColor:     color.solid,
        backgroundColor: color.alpha,
        pointBackgroundColor: color.solid,
        pointRadius:     4,
        pointHoverRadius: 6,
        fill:            false,
        tension:         0.4,
        borderWidth:     2,
      }],
    },
    options: {
      ...baseOptions(true),
      plugins: { ...baseOptions().plugins },
    },
  });
}

/**
 * Crée ou met à jour un graphique de zone (area chart).
 */
function renderAreaChart(canvasId, labels, data, label, color) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return;

  if (state.charts[canvasId]) state.charts[canvasId].destroy();

  state.charts[canvasId] = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label,
        data,
        borderColor:     color.solid,
        backgroundColor: color.alpha,
        pointBackgroundColor: color.solid,
        pointRadius:     3,
        pointHoverRadius: 5,
        fill:            true,
        tension:         0.4,
        borderWidth:     2,
      }],
    },
    options: {
      ...baseOptions(true),
      plugins: { ...baseOptions().plugins },
    },
  });
}

/* ========================================================================
   RENDU DES TABLEAUX
   ======================================================================== */

/**
 * Remplit le tableau des routes les plus chargées.
 * @param {Array} data
 */
function renderVehicleTable(data) {
  const max  = data[0]?.value || 1;
  const tbody = document.getElementById('table-vehicle-count');
  if (!tbody) return;

  tbody.innerHTML = data.map((row, i) => {
    const pct = (row.value / max * 100).toFixed(1);
    return `
      <tr>
        <td class="td-primary">${i + 1}</td>
        <td class="td-primary fw-600">${row.road_id}</td>
        <td>${fmt(row.value)}</td>
        <td style="width:140px">
          <div class="speed-bar">
            <div class="speed-bar-track">
              <div class="speed-bar-fill" style="width:${pct}%"></div>
            </div>
            <span style="font-size:11px;color:var(--color-text-muted);min-width:38px">${pct}%</span>
          </div>
        </td>
      </tr>
    `;
  }).join('');
}

/**
 * Remplit le tableau des routes les plus lentes.
 * @param {Array} data
 */
function renderSpeedTable(data) {
  const tbody = document.getElementById('table-avg-speed');
  if (!tbody) return;

  tbody.innerHTML = data.map((row, i) => {
    const speed    = row.value;
    const barClass = speed < 20 ? 'slow' : speed < 60 ? 'medium' : '';
    const pct      = Math.min(speed / 140 * 100, 100).toFixed(1);
    return `
      <tr>
        <td class="td-primary">${i + 1}</td>
        <td class="td-primary fw-600">${row.road_id}</td>
        <td>
          <div class="speed-bar">
            <div class="speed-bar-track">
              <div class="speed-bar-fill ${barClass}" style="width:${pct}%"></div>
            </div>
            <span style="font-size:12px;color:var(--color-text-secondary);min-width:55px">${speed} km/h</span>
          </div>
        </td>
      </tr>
    `;
  }).join('');
}

/**
 * Remplit le tableau de détection de congestion.
 * @param {Array} data
 */
function renderCongestionTable(data) {
  const tbody = document.getElementById('table-congestion');
  if (!tbody) return;

  tbody.innerHTML = data.map(row => {
    const badge = row.congested
      ? '<span class="badge badge-danger">🔴 Congestionnée</span>'
      : '<span class="badge badge-success">🟢 Fluide</span>';
    return `
      <tr>
        <td class="td-primary fw-600">${row.road_id}</td>
        <td>${row.avg_speed} km/h</td>
        <td>${fmt(row.vehicle_count)}</td>
        <td>${badge}</td>
      </tr>
    `;
  }).join('');
}

/* ========================================================================
   HORLOGE EN TEMPS RÉEL
   ======================================================================== */
function startClock() {
  const el = document.getElementById('clock');
  if (!el) return;

  const update = () => {
    el.textContent = new Date().toLocaleString('fr-FR', {
      day:    '2-digit', month: 'short', year: 'numeric',
      hour:   '2-digit', minute: '2-digit', second: '2-digit',
    });
  };
  update();
  setInterval(update, 1000);
}

/* ========================================================================
   NAVIGATION PAR SECTIONS
   ======================================================================== */
function initNav() {
  document.querySelectorAll('.nav-item[data-section]').forEach(item => {
    item.addEventListener('click', () => {
      // Mise à jour de l'état actif
      document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
      item.classList.add('active');

      // Affichage de la bonne section
      const target = item.dataset.section;
      document.querySelectorAll('.page-section').forEach(s => {
        s.classList.toggle('d-none', s.id !== `section-${target}`);
      });
    });
  });
}

/* ========================================================================
   ENTRÉE PRINCIPALE
   ======================================================================== */
document.addEventListener('DOMContentLoaded', () => {
  startClock();
  initNav();

  // Afficher immédiatement un état de chargement pour éviter l'attente visuelle
  toggleLoading('chart-vehicle-count', true);
  toggleLoading('kpi-total', true);
  toggleLoading('table-vehicle-count', true);
  toggleLoading('chart-avg-speed', true);
  toggleLoading('kpi-speed', true);
  toggleLoading('table-avg-speed', true);
  toggleLoading('chart-peak-hours', true);
  toggleLoading('kpi-congested', true);
  toggleLoading('table-congestion', true);

  // Chargement en parallèle sans attente bloquante
  Promise.all([
    loadStatus(),
    loadAllData()
  ]);

  // Rafraîchissement automatique du statut toutes les 30 secondes
  setInterval(loadStatus, 30_000);
});
