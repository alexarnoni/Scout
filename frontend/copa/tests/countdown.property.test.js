import { describe, it, expect } from 'vitest';
import fc from 'fast-check';

/**
 * Feature: copa-frontend, Property 1: Countdown calculation correctness
 * Validates: Requirements 4.1, 4.3
 *
 * Reimplementation of computeCountdown from frontend/copa/index.html
 * for isolated property-based testing.
 */
function computeCountdown(now, target) {
  const diff = target - now;
  if (diff <= 0) return null;
  const days = Math.floor(diff / 86400000);
  const hours = Math.floor((diff % 86400000) / 3600000);
  const minutes = Math.floor((diff % 3600000) / 60000);
  return { days, hours, minutes };
}

const TARGET = new Date('2026-06-11T19:00:00Z').getTime();

describe('Feature: copa-frontend, Property 1: Countdown calculation correctness', () => {
  it('decomposition into days/hours/minutes is a correct floor partition of the time difference', () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: TARGET - 1 }),
        (nowTimestamp) => {
          const result = computeCountdown(nowTimestamp, TARGET);
          expect(result).not.toBeNull();

          const { days, hours, minutes } = result;
          const reconstructed = days * 86400000 + hours * 3600000 + minutes * 60000;
          const diff = TARGET - nowTimestamp;

          // Verify: reconstructed <= diff < reconstructed + 60000
          expect(reconstructed).toBeLessThanOrEqual(diff);
          expect(diff).toBeLessThan(reconstructed + 60000);

          // Verify range constraints
          expect(days).toBeGreaterThanOrEqual(0);
          expect(hours).toBeGreaterThanOrEqual(0);
          expect(hours).toBeLessThan(24);
          expect(minutes).toBeGreaterThanOrEqual(0);
          expect(minutes).toBeLessThan(60);
        }
      ),
      { numRuns: 100 }
    );
  });

  it('returns null when now >= target', () => {
    fc.assert(
      fc.property(
        fc.integer({ min: TARGET, max: TARGET + 365 * 86400000 }),
        (nowTimestamp) => {
          const result = computeCountdown(nowTimestamp, TARGET);
          expect(result).toBeNull();
        }
      ),
      { numRuns: 100 }
    );
  });
});
