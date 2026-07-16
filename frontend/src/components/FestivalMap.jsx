import React, { useMemo } from 'react';
import { createPortal } from 'react-dom';
import { Users } from 'lucide-react';
import ZoneDetails from './ZoneDetails';

// Génère des positions aléatoires dans un cercle pour simuler une foule organique
const generateCrowd = (count, radius) => {
  const crowd = [];
  for (let i = 0; i < count; i++) {
    const r = radius * Math.sqrt(Math.random()) * 0.85;
    const theta = Math.random() * 2 * Math.PI;
    const x = r * Math.cos(theta);
    const y = r * Math.sin(theta);
    crowd.push({ id: i, x, y });
  }
  return crowd;
};

export default function FestivalMap({ zones, selectedZoneId, onSelectZone }) {
  if (!zones || zones.length === 0) return <div className="festival-map-loading">Chargement de la carte...</div>;

  const maxCapacity = Math.max(...zones.map(z => z.capacity));
  const selectedZone = zones.find(z => z.zone_id === selectedZoneId);

  return (
    <>
      <div className="festival-map-container">
        {zones.map(zone => {
          const isSelected = selectedZoneId === zone.zone_id;
          
          const sizeRatio = zone.capacity / maxCapacity;
          const radius = 60 + (sizeRatio * 80);
          const sizePx = radius * 2;

          const numPeople = Math.floor(zone.density * 50);
          const crowd = useMemo(() => generateCrowd(numPeople, radius), [numPeople, radius]);

          let statusColor = 'var(--success)';
          if (zone.density >= 0.85) statusColor = 'var(--danger)';
          else if (zone.density >= 0.60) statusColor = 'var(--warning)';

          return (
            <div 
              key={zone.zone_id}
              className={`map-scene-wrapper ${isSelected ? 'selected' : ''}`}
              onClick={() => onSelectZone(zone.zone_id)}
            >
              <div 
                className={`map-scene ${zone.density >= 0.85 ? 'pulse-danger' : ''}`}
                style={{
                  width: sizePx,
                  height: sizePx,
                  borderColor: statusColor,
                  boxShadow: isSelected ? `0 0 20px ${statusColor}` : 'none'
                }}
              >
                <div className="scene-stage" />
                <div className="crowd-container" style={{ transform: `translate(${radius}px, ${radius}px)` }}>
                  {crowd.map(person => (
                    <div 
                      key={person.id}
                      className="crowd-person"
                      style={{
                        transform: `translate(${person.x}px, ${person.y}px)`,
                        backgroundColor: statusColor
                      }}
                    />
                  ))}
                </div>
              </div>
              
              <div className="scene-label">
                <span className="scene-name">{zone.name}</span>
                <span className="scene-capacity"><Users size={12}/> {Math.round(zone.headcount_est)}</span>
              </div>
            </div>
          );
        })}
      </div>

      {/* Portal: la modale est rendue directement dans document.body, pas dans la map */}
      {selectedZone && createPortal(
        <div 
          className="zone-details-overlay"
          onClick={() => onSelectZone(null)}
        >
          <div 
            className="zone-details-popup"
            onClick={e => e.stopPropagation()} 
          >
            <ZoneDetails zone={selectedZone} onClose={() => onSelectZone(null)} />
          </div>
        </div>,
        document.body
      )}
    </>
  );
}
