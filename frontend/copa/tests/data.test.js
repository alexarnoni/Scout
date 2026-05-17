import { describe, it, expect } from 'vitest';

// Re-define the data here for isolated testing (same as in index.html)
const GROUPS = [
  { id: 'A', teams: [
    { flag: '🇺🇸', name: 'Estados Unidos' },
    { flag: '🇵🇦', name: 'Panamá' },
    { flag: '🇲🇽', name: 'México' },
    { flag: '🇷🇸', name: 'Sérvia' },
  ]},
  { id: 'B', teams: [
    { flag: '🇦🇷', name: 'Argentina' },
    { flag: '🇨🇱', name: 'Chile' },
    { flag: '🇵🇪', name: 'Peru' },
    { flag: '🇦🇺', name: 'Austrália' },
  ]},
  { id: 'C', teams: [
    { flag: '🇧🇷', name: 'Brasil' },
    { flag: '🇩🇪', name: 'Alemanha' },
    { flag: '🇯🇵', name: 'Japão' },
    { flag: '🇨🇮', name: 'Costa do Marfim' },
  ]},
  { id: 'D', teams: [
    { flag: '🇫🇷', name: 'França' },
    { flag: '🇲🇦', name: 'Marrocos' },
    { flag: '🇧🇪', name: 'Bélgica' },
    { flag: '🇿🇦', name: 'África do Sul' },
  ]},
  { id: 'E', teams: [
    { flag: '🇪🇸', name: 'Espanha' },
    { flag: '🇳🇱', name: 'Holanda' },
    { flag: '🇸🇳', name: 'Senegal' },
    { flag: '🇳🇿', name: 'Nova Zelândia' },
  ]},
  { id: 'F', teams: [
    { flag: '🏴󠁧󠁢󠁥󠁮󠁧󠁿', name: 'Inglaterra' },
    { flag: '🇵🇹', name: 'Portugal' },
    { flag: '🇲🇽', name: 'México' },
    { flag: '🇰🇪', name: 'Quênia' },
  ]},
  { id: 'G', teams: [
    { flag: '🇺🇾', name: 'Uruguai' },
    { flag: '🇨🇴', name: 'Colômbia' },
    { flag: '🇰🇷', name: 'Coreia do Sul' },
    { flag: '🇬🇭', name: 'Gana' },
  ]},
  { id: 'H', teams: [
    { flag: '🇵🇹', name: 'Portugal' },
    { flag: '🇭🇷', name: 'Croácia' },
    { flag: '🇹🇳', name: 'Tunísia' },
    { flag: '🇸🇦', name: 'Arábia Saudita' },
  ]},
  { id: 'I', teams: [
    { flag: '🇩🇰', name: 'Dinamarca' },
    { flag: '🇨🇦', name: 'Canadá' },
    { flag: '🇦🇹', name: 'Áustria' },
    { flag: '🇦🇴', name: 'Angola' },
  ]},
  { id: 'J', teams: [
    { flag: '🇨🇭', name: 'Suíça' },
    { flag: '🇵🇱', name: 'Polônia' },
    { flag: '🇪🇨', name: 'Equador' },
    { flag: '🇮🇶', name: 'Iraque' },
  ]},
  { id: 'K', teams: [
    { flag: '🇹🇷', name: 'Turquia' },
    { flag: '🇺🇦', name: 'Ucrânia' },
    { flag: '🇨🇲', name: 'Camarões' },
    { flag: '🇹🇭', name: 'Tailândia' },
  ]},
  { id: 'L', teams: [
    { flag: '🇮🇷', name: 'Irã' },
    { flag: '🇸🇪', name: 'Suécia' },
    { flag: '🇲🇱', name: 'Mali' },
    { flag: '🇵🇾', name: 'Paraguai' },
  ]},
];

const TEAM_MOCK = {
  'Brasil': {
    squad: {
      gk:  [{ name: 'Alisson', club: 'Liverpool' }, { name: 'Ederson', club: 'Man City' }, { name: 'Bento', club: 'Al-Nassr' }],
      def: [{ name: 'Marquinhos', club: 'PSG' }, { name: 'Militão', club: 'Real Madrid' }, { name: 'Danilo', club: 'Juventus' }, { name: 'Alex Telles', club: 'Sevilla' }],
      mid: [{ name: 'Bruno Guimarães', club: 'Newcastle' }, { name: 'Paquetá', club: 'West Ham' }, { name: 'Gerson', club: 'Flamengo' }, { name: 'Andrey Santos', club: 'Chelsea' }],
      fwd: [{ name: 'Vinícius Jr', club: 'Real Madrid' }, { name: 'Rodrygo', club: 'Real Madrid' }, { name: 'Endrick', club: 'Real Madrid' }, { name: 'Raphinha', club: 'Barcelona' }],
    },
    fixtures: [
      { date: '12 jun · 16h BRT', opponent: 'Alemanha 🇩🇪', venue: 'MetLife Stadium · New Jersey' },
      { date: '17 jun · 19h BRT', opponent: 'Costa do Marfim 🇨🇮', venue: 'SoFi Stadium · Los Angeles' },
      { date: '22 jun · 22h BRT', opponent: 'Japão 🇯🇵', venue: 'AT&T Stadium · Dallas' },
    ],
  }
};

describe('Mock data structure - GROUPS', () => {
  it('has exactly 12 groups', () => {
    expect(GROUPS).toHaveLength(12);
  });

  it('each group has exactly 4 teams', () => {
    GROUPS.forEach(g => {
      expect(g.teams).toHaveLength(4);
    });
  });

  it('total teams equals 48', () => {
    const total = GROUPS.reduce((sum, g) => sum + g.teams.length, 0);
    expect(total).toBe(48);
  });

  it('each team has name (string) and flag (string)', () => {
    GROUPS.forEach(g => {
      g.teams.forEach(t => {
        expect(typeof t.name).toBe('string');
        expect(t.name.length).toBeGreaterThan(0);
        expect(typeof t.flag).toBe('string');
        expect(t.flag.length).toBeGreaterThan(0);
      });
    });
  });

  it('groups are labeled A through L', () => {
    const ids = GROUPS.map(g => g.id);
    expect(ids).toEqual(['A','B','C','D','E','F','G','H','I','J','K','L']);
  });
});

describe('Mock data structure - TEAM_MOCK', () => {
  it('has squad with gk, def, mid, fwd arrays', () => {
    const squad = TEAM_MOCK['Brasil'].squad;
    expect(Array.isArray(squad.gk)).toBe(true);
    expect(Array.isArray(squad.def)).toBe(true);
    expect(Array.isArray(squad.mid)).toBe(true);
    expect(Array.isArray(squad.fwd)).toBe(true);
  });

  it('each player has name and club strings', () => {
    const squad = TEAM_MOCK['Brasil'].squad;
    const allPlayers = [...squad.gk, ...squad.def, ...squad.mid, ...squad.fwd];
    allPlayers.forEach(p => {
      expect(typeof p.name).toBe('string');
      expect(p.name.length).toBeGreaterThan(0);
      expect(typeof p.club).toBe('string');
      expect(p.club.length).toBeGreaterThan(0);
    });
  });

  it('has fixtures array with date, opponent, venue', () => {
    const fixtures = TEAM_MOCK['Brasil'].fixtures;
    expect(Array.isArray(fixtures)).toBe(true);
    expect(fixtures.length).toBeGreaterThan(0);
    fixtures.forEach(f => {
      expect(typeof f.date).toBe('string');
      expect(typeof f.opponent).toBe('string');
      expect(typeof f.venue).toBe('string');
    });
  });
});
