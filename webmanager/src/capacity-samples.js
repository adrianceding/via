export const CAPACITY_SAMPLE_WINDOW_MS = 250;

export function concurrentCapacityTotal(samples, field = 'capacity') {
  if (samples.length === 0 || !samples.every((sample) => {
    const value = sample[field];
    return value != null && Number.isFinite(value) && value >= 0;
  })) return null;
  if (samples.length > 1) {
    const ages = samples.map((sample) => sample.ageMs == null ? Number.NaN : Number(sample.ageMs));
    if (!ages.every((age) => Number.isFinite(age) && age >= 0)
      || Math.max(...ages) - Math.min(...ages) > CAPACITY_SAMPLE_WINDOW_MS) return null;
  }
  return samples.reduce((total, sample) => total + sample[field], 0);
}