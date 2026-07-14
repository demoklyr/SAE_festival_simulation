import React from 'react';

export default function ZoneCard({ zone }) {
  const { name, headcount_est, capacity, density, avg_speed } = zone;
  
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
    </div>
  );
}
