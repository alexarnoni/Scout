import { describe, it, expect } from 'vitest';

// Re-implement functions for isolated testing
const TEAM_MOCK = {
  'Brasil': {
    squad: {
      gk:  [{ name: 'Alisson', club: 'Liverpool' }],
      def: [{ name: 'Marquinhos', club: 'PSG' }],
      mid: [{ name: 'Bruno Guimarães', club: 'Newcastle' }],
      fwd: [{ name: 'Vinícius Jr', club: 'Real Madrid' }],
    },
    fixtures: [
      { date: '12 jun · 16h BRT', opponent: 'Alemanha 🇩🇪', venue: 'MetLife Stadium · New Jersey' },
    ],
  }
};

function buildDrawerContent(teamName) {
  const data = TEAM_MOCK[teamName];

  const squadHTML = data
    ? `
      <div class="pos-group">
        <span class="pos-label pos-gk">GOL</span>
        ${data.squad.gk.map(p => `<div class="player-row">${p.name}<span class="player-club">· ${p.club}</span></div>`).join('')}
      </div>
      <div class="pos-group">
        <span class="pos-label pos-def">DEF</span>
        ${data.squad.def.map(p => `<div class="player-row">${p.name}<span class="player-club">· ${p.club}</span></div>`).join('')}
      </div>
      <div class="pos-group">
        <span class="pos-label pos-mid">MEI</span>
        ${data.squad.mid.map(p => `<div class="player-row">${p.name}<span class="player-club">· ${p.club}</span></div>`).join('')}
      </div>
      <div class="pos-group">
        <span class="pos-label pos-fwd">ATA</span>
        ${data.squad.fwd.map(p => `<div class="player-row">${p.name}<span class="player-club">· ${p.club}</span></div>`).join('')}
      </div>`
    : '<div class="drawer-empty">Elenco ainda não anunciado</div>';

  const fixturesHTML = data
    ? data.fixtures.map(f => `
        <div class="fixture-card">
          <div class="fixture-date">${f.date}</div>
          <div class="fixture-teams">🇧🇷 Brasil vs ${f.opponent}</div>
          <div class="fixture-venue">${f.venue}</div>
        </div>`).join('')
    : '<div class="drawer-empty">Agenda a confirmar</div>';

  return `
    <div class="drawer-section">
      <div class="drawer-section-title">👕 Convocados</div>
      ${squadHTML}
    </div>
    <div class="drawer-section">
      <div class="drawer-section-title">📅 Próximos jogos</div>
      ${fixturesHTML}
    </div>
    <div class="drawer-section">
      <div class="drawer-section-title">📊 Resultados</div>
      <div class="drawer-empty">Copa ainda não começou</div>
    </div>
  `;
}

function computeCountdown(now, target) {
  const diff = target - now;
  if (diff <= 0) return null;
  const days = Math.floor(diff / 86400000);
  const hours = Math.floor((diff % 86400000) / 3600000);
  const minutes = Math.floor((diff % 3600000) / 60000);
  return { days, hours, minutes };
}

describe('buildDrawerContent', () => {
  it('includes position badges for Brasil', () => {
    const html = buildDrawerContent('Brasil');
    expect(html).toContain('pos-gk');
    expect(html).toContain('pos-def');
    expect(html).toContain('pos-mid');
    expect(html).toContain('pos-fwd');
    expect(html).toContain('GOL');
    expect(html).toContain('DEF');
    expect(html).toContain('MEI');
    expect(html).toContain('ATA');
  });

  it('includes player names and clubs for Brasil', () => {
    const html = buildDrawerContent('Brasil');
    expect(html).toContain('Alisson');
    expect(html).toContain('Liverpool');
    expect(html).toContain('Vinícius Jr');
    expect(html).toContain('Real Madrid');
  });

  it('includes fixture data for Brasil', () => {
    const html = buildDrawerContent('Brasil');
    expect(html).toContain('Alemanha');
    expect(html).toContain('MetLife Stadium');
    expect(html).toContain('12 jun');
  });

  it('shows "Elenco ainda não anunciado" for unknown team', () => {
    const html = buildDrawerContent('Argentina');
    expect(html).toContain('Elenco ainda não anunciado');
  });

  it('shows "Agenda a confirmar" for unknown team fixtures', () => {
    const html = buildDrawerContent('Argentina');
    expect(html).toContain('Agenda a confirmar');
  });
});

describe('computeCountdown', () => {
  const TARGET = new Date('2026-06-11T19:00:00Z').getTime();

  it('returns null when date is past target', () => {
    const result = computeCountdown(TARGET + 1000, TARGET);
    expect(result).toBeNull();
  });

  it('returns null when date equals target', () => {
    const result = computeCountdown(TARGET, TARGET);
    expect(result).toBeNull();
  });

  it('returns correct values for known difference', () => {
    // 1 day, 2 hours, 30 minutes before target
    const diff = 1 * 86400000 + 2 * 3600000 + 30 * 60000;
    const result = computeCountdown(TARGET - diff, TARGET);
    expect(result).toEqual({ days: 1, hours: 2, minutes: 30 });
  });

  it('returns 0 minutes for exact hour boundary', () => {
    const diff = 3 * 86400000 + 5 * 3600000;
    const result = computeCountdown(TARGET - diff, TARGET);
    expect(result).toEqual({ days: 3, hours: 5, minutes: 0 });
  });
});
