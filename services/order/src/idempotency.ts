export function validateIdempotencyKey(value: string | undefined): string {
  if (!value || value.length < 8 || value.length > 128) throw new Error('Idempotency-Key must be 8-128 characters');
  return value;
}
