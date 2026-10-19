import http from 'k6/http';
import { Counter } from 'k6/metrics';
import { check, fail } from 'k6';

const baseURL = __ENV.BASE_URL || 'http://gateway:8080';
const vus = Number(__ENV.VUS || 100);
const duration = __ENV.DURATION || '5s';
const accepted = new Counter('accepted_202');

http.setResponseCallback(http.expectedStatuses(200, 201, 202));

export const options = {
  vus,
  duration,
  summaryTrendStats: ['min', 'med', 'avg', 'p(90)', 'p(95)', 'p(99)', 'max'],
  thresholds: { http_req_failed: ['rate==0'], checks: ['rate>0.99'] },
};

export function setup() {
  const product = http.post(`${baseURL}/api/v1/admin/product`, JSON.stringify({ name: `m4-kafka-${Date.now()}`, price: '99.99', stock: 100000 }), { headers: { 'Content-Type': 'application/json' } });
  if (!check(product, { 'product created': (r) => r.status === 201 })) fail(product.body);
  const productID = product.json('product.id');
  const activity = http.post(`${baseURL}/api/v1/admin/activity`, JSON.stringify({ product_id: productID, start_time: new Date(Date.now() - 60_000).toISOString(), end_time: new Date(Date.now() + 3_600_000).toISOString(), seckill_price: '9.99', stock: 100000 }), { headers: { 'Content-Type': 'application/json' } });
  if (!check(activity, { 'activity created': (r) => r.status === 201 })) fail(activity.body);
  const activityID = activity.json('activity.id');
  const warmup = http.post(`${baseURL}/api/v1/admin/activity/${activityID}/warmup`);
  if (!check(warmup, { 'stock warmed': (r) => r.status === 200 })) fail(warmup.body);
  return { productID, activityID };
}

export default function (data) {
  const userID = __VU * 10_000_000 + __ITER + 1;
  const response = http.post(`${baseURL}/api/v1/seckill`, JSON.stringify({ activity_id: data.activityID, product_id: data.productID, user_id: userID }), { headers: { 'Content-Type': 'application/json' } });
  if (response.status === 202) accepted.add(1);
  check(response, { 'order accepted asynchronously': (r) => r.status === 202 && r.json('code') === 0 });
}
