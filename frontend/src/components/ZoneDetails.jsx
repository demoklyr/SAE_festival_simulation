import React, { useState, useEffect } from 'react';
import { Users, Navigation, Activity, X } from 'lucide-react';

export default function ZoneDetails({ zone, onClose }) {
  if (!zone) return null;

  const { name, headcount_est, capacity, density, avg_speed, zone_id } = zone;
  const [prediction, setPrediction] = useState(null);
  const [loadingPred, setLoadingPred] = useState(false);
  
  // Ressources exclusives à cette zone
  const [resources, setResources] = useState([]);

  const fetchResources = async () => {
    try {
      const res = await fetch('http://localhost:8001/resources');
      if (res.ok) {
        const data = await res.json();
        // Filtrer pour ne garder que les ressources de CETTE zone
        setResources(data.filter(r => r.zone_id === zone_id));
      }
    } catch (e) {
      console.error(e);
    }
  };

  useEffect(() => {
    fetchResources();
    setPrediction(null); // Reset prediction when zone changes
    const interval = setInterval(fetchResources, 5000);
    return () => clearInterval(interval);
  }, [zone_id]);

  const restock = async (resource_id) => {
    try {
      await fetch(`http://localhost:8001/resources/${resource_id}/restock`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ level_pct: 100 })
      });
      fetchResources();
    } catch (e) {
      console.error(e);
    }
  };

  const fetchPrediction = async () => {
    setLoadingPred(true);
    try {
      const res = await fetch(`http://localhost:8001/predict/${zone_id}?horizon_minutes=30`);
      if (res.ok) setPrediction(await res.json());
      else alert((await res.json()).detail);
    } catch (e) { console.error(e); }
    setLoadingPred(false);
  };

  let statusColor = 'var(--success)';
  if (density >= 0.85) statusColor = 'var(--danger)';
  else if (density >= 0.60) statusColor = 'var(--warning)';

  const barWidth = Math.min(density * 100, 100).toFixed(1);

  return (
    <div className="zone-details-panel">
      <div className="zone-details-header">
        <h2>{name}</h2>
        <button className="close-btn" onClick={onClose}><X size={20} /></button>
      </div>

      <div className="zone-stats">
        <div className="stat">
          <span className="label"><Users size={14} /> Foule</span>
          <span className="value">{Math.round(headcount_est)} <span className="sub-value">/ {capacity}</span></span>
        </div>
        <div className="stat">
          <span className="label"><Navigation size={14} /> Vitesse</span>
          <span className="value">{avg_speed ? avg_speed.toFixed(2) : '0.00'} <span className="sub-value">m/s</span></span>
        </div>
      </div>

      <div className="gauge-wrapper" style={{ marginTop: '20px', marginBottom: '20px' }}>
        <div className="gauge-header">
          <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}><Activity size={14} /> Densité</span>
          <span style={{ color: statusColor, fontWeight: 'bold' }}>{barWidth}%</span>
        </div>
        <div className="gauge-container">
          <div className="gauge-fill" style={{ width: `${barWidth}%`, backgroundColor: statusColor }} />
        </div>
      </div>

      <div className="prediction-section">
        <button className="action-btn" onClick={fetchPrediction} disabled={loadingPred}>
          {loadingPred ? 'Calcul IA...' : 'Prédire l\'affluence (30m)'}
        </button>
        {prediction && (
          <div className="prediction-result">
            <strong>Prévision :</strong> {(prediction.predicted_density * 100).toFixed(1)}% <br />
            <strong>Intervalle :</strong> {(prediction.confidence_interval[0] * 100).toFixed(1)}% - {(prediction.confidence_interval[1] * 100).toFixed(1)}%
          </div>
        )}
      </div>

      <hr className="divider" />

      <div className="logistics-section">
        <h3>Logistique de la scène</h3>
        {resources.length === 0 ? (
          <p className="no-data">Aucune ressource liée à cette zone.</p>
        ) : (
          <ul className="resource-list">
            {resources.map(r => (
              <li key={r.resource_id} className="resource-item">
                <div className="resource-info">
                  <span className="resource-name">{r.resource_id}</span>
                  <span className="resource-pct" style={{ color: r.stock_level_pct < 15 ? 'var(--danger)' : 'var(--text-secondary)' }}>
                    {r.stock_level_pct !== null ? r.stock_level_pct.toFixed(1) : 0}%
                  </span>
                </div>
                <button className="restock-btn" onClick={() => restock(r.resource_id)}>Restock</button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
