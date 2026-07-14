import React from 'react';

export default function LiveAlerts({ alerts }) {
  if (!alerts || alerts.length === 0) {
    return (
      <div className="alerts-panel">
        <h2>Dernières Alertes</h2>
        <div className="no-alerts">
          Aucune alerte active ✅
        </div>
      </div>
    );
  }

  return (
    <div className="alerts-panel">
      <h2>Dernières Alertes ({alerts.length})</h2>
      <div className="alerts-list">
        {alerts.map((alert) => {
          const isCritical = alert.severity === 'CRITICAL';
          
          return (
            <div key={alert.alert_id} className={`alert-item ${isCritical ? 'alert-critical' : 'alert-warning'}`}>
              <div className="alert-header">
                <span className="alert-type">{alert.type ? alert.type.toUpperCase() : 'ALERTE'}</span>
                <span className="alert-time">
                  {new Date(alert.ts).toLocaleTimeString()}
                </span>
              </div>
              <div className="alert-zone">Zone : {alert.zone_id}</div>
              <p className="alert-message">{alert.recommended_action}</p>
            </div>
          );
        })}
      </div>
    </div>
  );
}
