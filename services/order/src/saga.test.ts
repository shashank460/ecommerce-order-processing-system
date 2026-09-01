import { describe, expect, it } from 'vitest';

type Item = { productId: number; quantity: number };

function reserve(stock: Map<number, number>, items: Item[]) {
  const next = new Map(stock);
  for (const item of items) {
    const available = next.get(item.productId) ?? 0;
    if (available < item.quantity) return { ok: false, stock };
    next.set(item.productId, available - item.quantity);
  }
  return { ok: true, stock: next };
}

function release(stock: Map<number, number>, items: Item[]) {
  const next = new Map(stock);
  for (const item of items) next.set(item.productId, (next.get(item.productId) ?? 0) + item.quantity);
  return next;
}

describe('order saga failure paths', () => {
  it('ignores duplicate event delivery', () => {
    const processed = new Set<string>();
    const handle = (eventId: string) => {
      if (processed.has(eventId)) return false;
      processed.add(eventId);
      return true;
    };
    expect(handle('evt-1')).toBe(true);
    expect(handle('evt-1')).toBe(false);
    expect(processed.size).toBe(1);
  });

  it('rejects insufficient stock without partially reserving items', () => {
    const stock = new Map([[1, 2], [2, 10]]);
    const result = reserve(stock, [{ productId: 1, quantity: 3 }, { productId: 2, quantity: 1 }]);
    expect(result.ok).toBe(false);
    expect([...result.stock]).toEqual([[1, 2], [2, 10]]);
  });

  it('releases the exact reservation after payment failure', () => {
    const initial = new Map([[1, 10]]);
    const items = [{ productId: 1, quantity: 4 }];
    const reserved = reserve(initial, items);
    expect(reserved.ok).toBe(true);
    const compensated = release(reserved.stock, items);
    expect(compensated.get(1)).toBe(10);
  });
});
