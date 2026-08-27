import { randomUUID } from 'node:crypto';

export type EventType =
  | 'OrderCreated'
  | 'InventoryReserved'
  | 'InventoryReleased'
  | 'InventoryFailed'
  | 'PaymentCompleted'
  | 'PaymentFailed'
  | 'OrderConfirmed'
  | 'OrderCancelled';

export interface DomainEvent<T = Record<string, unknown>> {
  eventId: string;
  eventType: EventType;
  version: 1;
  occurredAt: string;
  correlationId: string;
  payload: T;
}

export interface OrderCreatedPayload {
  orderId: string;
  userId: string;
  items: { productId: number; quantity: number }[];
}

export interface InventoryPayload extends OrderCreatedPayload {}
export interface PaymentPayload extends OrderCreatedPayload {}

export function parseEvent<T = Record<string, unknown>>(value: string): DomainEvent<T> {
  const event = JSON.parse(value) as DomainEvent<T>;
  if (!event.eventId || !event.eventType || event.version !== 1 || !event.occurredAt || !event.correlationId || !event.payload) {
    throw new Error('Invalid event contract');
  }
  return event;
}

export function createEvent<T>(eventType: EventType, payload: T, correlationId: string): DomainEvent<T> {
  return {
    eventId: randomUUID(),
    eventType,
    version: 1,
    occurredAt: new Date().toISOString(),
    correlationId,
    payload,
  };
}
