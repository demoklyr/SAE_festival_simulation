import React, { useState } from 'react';

export default function ZoneCard({ zone }) {
  const { name, headcount_est, capacity, density, avg_speed, zone_id } = zone;
  const [prediction, setPrediction] = useState(null);
  const [loading, setLoading] = useState(false);
  
  // Déterminer la couleur de la jauge en fonction de la densité
  let statusColor = '#4caf50'; // Vert (Safe)
  let isCritical = false;
  
  if (density >= 0.10) {
    statusColor = '#f44336'; // Rouge (Critical)
    isCritical = true;
  } else if (density >= 0.05) {
    statusColor = '#ff9800'; // Orange (Watch)
  }

  // Largeur de la jauge (max 100%)
  const barWidth = Math.min(density * 100, 100).toFixed(1);

  const fetchPrediction = async () => {
    setLoading(true);
    try {
      const res = await fetch(`http://localhost:8001/predict/${zone_id}?horizon_minutes=30`);
      if (res.ok) {
        const data = await res.json();
        setPrediction(data);
      } else {
        const err = await res.json();
        alert(err.detail);
      }
    } catch (e) {
      console.error(e);
    }
    setLoading(false);
  };

  return (
    <div className={`zone-card ${isCritical ? 'critical-pulse' : ''}`}>
      <div className="zone-header">
        <h3>{name}</h3>
        <span className="zone-speed">Vitesse moy. : {avg_speed ? avg_speed.toFixed(2) : 0} m/s</span>
      </div>
      
      <div className="zone-stats">
        <div className="stat">
          <span className="label">Foule</span>
          <span className="value">{Math.round(headcount_est)} / {capacity}</span>
        </div>
        <div className="stat">
          <span className="label">Densité</span>
          <span className="value" style={{ color: statusColor }}>
            {(density * 100).toFixed(1)}%
          </span>
        </div>
      </div>

      <div className="gauge-container">
        <div 
          className="gauge-fill" 
          style={{ 
            width: `${barWidth}%`, 
            backgroundColor: statusColor 
          }} 
        />
      </div>

      <div style={{ marginTop: '15px', paddingTop: '10px', borderTop: '1px solid rgba(255, 255, 255, 0.1)' }}>
        <button onClick={fetchPrediction} disabled={loading} style={{ cursor: 'pointer', padding: '5px 10px', background: 'rgba(255,255,255,0.1)', color: 'white', border: '1px solid rgba(255,255,255,0.2)', borderRadius: '4px' }}>
          {loading ? 'Calcul...' : 'Prédire (30m)'}
        </button>
        {prediction && (
          <div style={{ marginTop: '8px', fontSize: '0.85rem', color: '#94a3b8' }}>
            <strong>Prévision :</strong> {(prediction.predicted_density * 100).toFixed(1)}% <br />
            <strong>Intervalle :</strong> {(prediction.confidence_interval[0] * 100).toFixed(1)}% - {(prediction.confidence_interval[1] * 100).toFixed(1)}%
          </div>
        )}
      </div>
    </div>
  );
}
