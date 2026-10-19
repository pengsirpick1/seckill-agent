import http from 'k6/http';
import { Counter } from 'k6/metrics';
import { check, fail } from 'k6';

const baseURL = __ENV.BASE_URL || 'http://gateway:8080';
const vus = Number(__ENV.VUS || 100);
const duration = __ENV.DURATION || '5s';
const productCount = 20;
const accepted = new Counter('accepted_202');
const lockAcquireFailures = new Counter('etcd_lock_acquire_failures');

http.setResponseCallback(http.expectedStatuses(200, 201, 202));

export const options = {
  vus,
  duration,
  summaryTrendStats: ['min', 'med', 'avg', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  const items = [];
  const suffix = Date.now();
  for (let index = 0; index < productCount; index += 1) {
    const product = http.post(`${baseURL}/api/v1/admin/product`, JSON.stringify({ name: `m5-etcd-multi-${suffix}-${index}`, price: '99.99', stock: 100000 }), { headers: { 'Content-Type': 'application/json' } });
    if (!check(product, { 'product created': (r) => r.status === 201 })) fail(product.body);
    const productID = product.json('product.id');
    const activity = http.post(`${baseURL}/api/v1/admin/activity`, JSON.stringify({ product_id: productID, start_time: new Date(Date.now() - 60_000).toISOString(), end_time: new Date(Date.now() + 3_600_000).toISOString(), seckill_price: '9.99', stock: 100000 }), { headers: { 'Content-Type': 'application/json' } });
    if (!check(activity, { 'activity created': (r) => r.status === 201 })) fail(activity.body);
    const activityID = activity.json('activity.id');
    const warmup = http.post(`${baseURL}/api/v1/admin/activity/${activityID}/warmup`);
    if (!check(warmup, { 'stock warmed': (r) => r.status === 200 })) fail(warmup.body);
    items.push({ productID, activityID });
  }
  return { items };
}

export default function (data) {
  const item = data.items[(__VU + __ITER) % productCount];
  const userID = __VU * 10_000_000 + __ITER + 1;
  const response = http.post(`${baseURL}/api/v1/seckill`, JSON.stringify({ activity_id: item.activityID, product_id: item.productID, user_id: userID }), { headers: { 'Content-Type': 'application/json' } });
  if (response.status === 202) accepted.add(1);
  if (response.status === 503 && response.json('code') === 'ETCD_LOCK_ACQUIRE_FAILED') lockAcquireFailures.add(1);
  check(response, { 'order accepted asynchronously': (r) => r.status === 202 && r.json('code') === 0 });
}
