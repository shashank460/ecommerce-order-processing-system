import { createEvent, parseEvent, type DomainEvent, type EventType, type OrderCreatedPayload } from '../../../contracts/events.js';

export { createEvent, parseEvent };
export type { DomainEvent, EventType, OrderCreatedPayload };

export function getOrderId(event: DomainEvent): string {
  const payload = event.payload as Partial<OrderCreatedPayload>;
  if (!payload.orderId) throw new Error('Event payload missing orderId');
  return payload.orderId;
}
